#!/usr/bin/env python
# This file is part of transfer_embargo
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import hashlib
import json
import logging
import os
import random
import tempfile
import time
import yaml
import zipfile
import zlib

import pydantic
import rucio.common.exception
from astro_metadata_translator.indexing import index_files
from astropy.time import Time, TimeDelta
from lsst.daf.butler import Butler, DimensionRecord, Timespan
from lsst.daf.butler.cli.cliLog import CliLog
from lsst.resources import ResourcePath
from lsst.utils.timer import time_this
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient


class DataQuery(pydantic.BaseModel):
    """A query to select exposures and their associated embargo time."""

    instrument: str
    """Instrument this query pertains to."""

    where: str
    """Where clause expression to select datasets to transfer."""

    embargo_hours: float
    """How long to embargo the selected datasets (hours)."""


class RucioInterface:
    """Register files in Rucio and attach them to datasets.

    Parameters
    ----------
    rucio_rse: `str`
        Name of the RSE that the files live in.
    scope: `str`
        Rucio scope to register the files in.
    """

    def __init__(self, rucio_rse: str, scope: str):
        self.rucio_rse = rucio_rse
        self.scope = scope

        self.replica_client = ReplicaClient()
        self.did_client = DIDClient()

    @classmethod
    def compute_hashes(cls, path: str) -> tuple[int, str, str]:
        """Compute the length, MD5, and Adler32 hashes for a file.

        Parameters
        ----------
        path: `str`
            Path to the file.

        Returns
        -------
        hashes: `tuple` [ `int`, `str`, `str` ]
            Size in bytes, MD5 hex, and Adler32 hex hashes.
        """
        size = 0
        md5 = hashlib.md5()
        adler32 = zlib.adler32(b"")
        with open(path, "rb") as f:
            while buffer := f.read(10 * 1024 * 1024):
                size += len(buffer)
                md5.update(buffer)
                adler32 = zlib.adler32(buffer, adler32)
        md5 = md5.hexdigest()
        adler32 = f"{adler32:08x}"
        return (size, md5, adler32)

    def _make_did(
        self, zip_path: str, hashes: tuple[int, str, str], meta: dict | None = None
    ) -> dict[str, str | int]:
        """Make a Rucio data identifier dictionary from a zip file.

        Parameters
        ----------
        zip_path: `str`
            Root-relative path to the file.
        hashes: `tuple` [ `int`, `str`, `str` ]
            Length, MD5, and Adler32 hashes of the file.
        meta: `dict` | None
            Optional metadata dictionary to send to Rucio.

        Returns
        -------
        did: `dict [ str, str|int ]`
            Rucio data identifier including physical and logical names,
            byte length, adler32 and MD5 checksums, and scope.
        """
        return dict(
            name=zip_path.removeprefix("/"),
            bytes_=hashes[0],
            md5=hashes[1],
            adler32=hashes[2],
            scope=self.scope,
            meta=meta,
        )

    def _compute_datasets(
        self, tracts: set[int], instrument: str, day_obs: int, obs_id: str
    ) -> set[str]:
        """Generate standardized Rucio Dataset names for raw data.

        Encapsulates the Dataset naming pattern.

        Parameters
        ----------
        tracts: `set` [ `int` ]
            Set of tracts to convert.  Empty if not on-sky.
        instrument: `str`
            Instrument to use.
        day_obs: `int`
            Observation day.
        obs_id: `str`
            Observation id.

        Returns
        -------
        datasets: `set` [ `str` ]
            Rucio Dataset names.
        """
        datasets = set()
        if tracts:
            for tract in tracts:
                dataset_id = f"Dataset/{instrument}/raw/{tract}/{day_obs}/{obs_id}"
                datasets.add(dataset_id)
        else:
            dataset_id = f"Dataset/{instrument}/raw/NoTract/{day_obs}/{obs_id}"
            datasets.add(dataset_id)
        return datasets

    def _add_replica(self, did: dict[str, int | str], dry_run: bool) -> None:
        """Add a file as a replica of a specified Rucio DID.

        Parameters
        ----------
        did: `dict` [ `str`, `int`|`str` ]
            Rucio DID as a dictionary.
        dry_run: `bool`
            If true, only log, do not write anything.
        """
        global logger

        logger.info("Adding replica to %s: %s", self.rucio_rse, did)
        if dry_run:
            return
        retries = 0
        max_retries = 2
        while True:
            try:
                self.replica_client.add_replica(rse=self.rucio_rse, pfn=None, **did)
                return
            except rucio.common.exception.FileAlreadyExists:
                return
            except rucio.common.exception.DatabaseException:
                logger.info("Retrying add_replica due to database")
                retries += 1
                if retries < max_retries:
                    time.sleep(random.uniform(0.5, 2))
                    continue
                else:
                    raise

    def _add_file_to_dataset(self, did: dict, dataset_id: str, dry_run: bool) -> None:
        """Attach a file specified by a Rucio DID to a Rucio dataset.

        Ignores already-attached files for idempotency.

        Parameters
        ----------
        did: `dict` [ `str`, `str`|`int` ]`
            Rucio data identifier.
        dataset_id: `str`
            Logical name of the Rucio dataset.
        dry_run: `bool`
            If true, only log, do not write anything.
        """
        global logger

        logger.info(
            "Registering %s in dataset %s, RSE %s", did, dataset_id, self.rucio_rse
        )
        if dry_run:
            return
        retries = 0
        max_retries = 2
        while True:
            try:
                self.did_client.add_files_to_dataset(
                    scope=self.scope,
                    name=dataset_id,
                    files=[{"scope": did["scope"], "name": did["name"]}],
                    rse=self.rucio_rse,
                )
                return
            except rucio.common.exception.FileAlreadyExists:
                return
            except rucio.common.exception.DataIdentifierNotFound:
                # No such dataset, so create it
                try:
                    logger.info("Creating Rucio dataset %s", dataset_id)
                    self.did_client.add_dataset(
                        scope=self.scope,
                        name=dataset_id,
                        statuses={"monotonic": True},
                        rse=self.rucio_rse,
                    )
                except rucio.common.exception.DataIdentifierAlreadyExists:
                    # If someone else created it in the meantime
                    pass
                # And then retry adding DIDs
                logger.info("Retrying add_files_to_dataset after creation")
                self._add_file_to_dataset(did, dataset_id, dry_run)
                return
            except rucio.common.exception.DatabaseException:
                logger.info("Retrying add_files_to_dataset due to database")
                retries += 1
                if retries < max_retries:
                    time.sleep(random.uniform(0.5, 2))
                    continue
            else:
                raise

    def register(
        self, name: str, hashes: tuple[int, str, str], tracts: set[int], dry_run: bool
    ) -> None:
        """Register a file in Rucio.

        Parameters
        ----------
        name: `str`
            Rucio Logical File Name (LFN) in instrument/day_obs/filename form.
        hashes: `tuple` [ `int`, `str`, `str` ]
            Length, MD5, and Adler32 hashes of the file.
        tracts: `set` [ `int` ]
            Set of tracts that the file overlaps (empty if not on-sky).
        dry_run: `bool`
            If true, only log, do not write anything.
        """
        instrument, day_obs, filename = name.split("/")
        obs_id = filename[:20]
        if name.endswith(".zip"):
            meta = {"rubin_butler": "zip_file"}
        else:
            meta = None
        did = self._make_did(name, hashes, meta)
        self._add_replica(did, dry_run)
        datasets = self._compute_datasets(tracts, instrument, day_obs, obs_id)
        for dataset in datasets:
            self._add_file_to_dataset(did, dataset, dry_run)


def parse_args():
    """Parses, validates, and returns command-line arguments.

    Returns
    -------
    ns : argparse.Namespace
        An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Transfer raw LSSTCam exposures from embargo butler"
            " to another butler, creating zip files."
        )
    )
    parser.add_argument(
        "fromrepo",
        type=str,
        help="Butler repository from which data is transferred.",
    )
    parser.add_argument(
        "torepo",
        type=str,
        help="Repository to which data is transferred.",
    )

    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Do not transfer any data or metadata; only log.",
    )

    parser.add_argument(
        "--window",
        type=str,
        required=False,
        help=(
            "Time to search past the embargo time period."
            " Specified in astropy quantity_str format (e.g. '3hr 5min 20s')."
            " Defines a window of data that will be transferred."
            " This is helpful for transferring a limited amount of data"
            " during the initial testing of the deployment."
        ),
    )
    parser.add_argument(
        "--now",
        default=None,
        type=str,
        help=(
            "Now time in (ISOT, TAI timescale)."
            " If left blank it will use astropy.time.Time.now."
        ),
    )

    parser.add_argument(
        "-C",
        "--config_file",
        required=True,
        type=str,
        help="Path to YAML config file.",
    )

    parser.add_argument(
        "-d",
        "--dest_uri_prefix",
        type=str,
        default="/sdf/data/rubin/lsstdata/offline/instrument/",
        help="Destination URI prefix for raw data.",
    )

    parser.add_argument(
        "-r",
        "--rucio_rse",
        type=str,
        required=False,
        help="Rucio RSE for raw data.",
    )
    parser.add_argument(
        "-s",
        "--scope",
        type=str,
        required=False,
        help="Rucio scope for raw data.",
    )

    parser.add_argument(
        "--log",
        type=str,
        required=False,
        help="Level name (WARNING, INFO, DEBUG) or comma-sepaarated list of logger=level pairs.",
    )

    ns = parser.parse_args()
    ns.now = Time(ns.now, format="isot", scale="tai") if ns.now else Time.now()
    if ns.now > Time.now():
        raise ValueError(f"--now is in the future: {ns.now}")
    if ns.rucio_rse is not None:
        if ns.scope is None:
            raise ValueError("--scope required with --rucio_rse")

    return ns


def transfer_data_query(data_query: DataQuery) -> None:
    """Transfer all files matching a data query into zip files.

    Parameters
    ----------
    data_query: `DataQuery`
        The query and associated embargo time.
    """
    global logger, config, source_butler

    # End of window is now - embargo length
    end_time = config.now - TimeDelta(data_query.embargo_hours * 3600, format="sec")
    # If window is defined, then start is that much before the end
    # Otherwise, start is infinitely previous
    if config.window is not None:
        start_time = end_time - TimeDelta(config.window, format="quantity_str")
    else:
        start_time = None
    ok_timespan = Timespan(start_time, end_time)

    # Find all exposures meeting criteria
    dim_where = "(exposure.timespan OVERLAPS :ok_timespan)"
    dim_where += f" AND ({data_query.where})" if data_query.where else ""
    dim_bind = {"ok_timespan": ok_timespan}
    logger.info("Querying exposure: %s with %s", dim_where, dim_bind)
    exposures = source_butler.query_dimension_records(
        "exposure",
        where=dim_where,
        bind=dim_bind,
        limit=None,
        instrument=data_query.instrument,
        order_by="exposure",
        explain=False,
    )
    if not exposures:
        logger.info("No matching records")
        return
    logger.info(
        "Result is %d exposures from %s to %s",
        len(exposures),
        exposures[0].id,
        exposures[-1].id,
    )

    for exp in exposures:
        process_exposure(exp, data_query.instrument)


def process_exposure(exp: DimensionRecord, instrument: str) -> None:
    """Process an exposure by zipping, ingesting, and registering it in Rucio.

    Parameters
    ----------
    exp: `lsst.daf.butler.DimensionRecord`
        The exposure to process.
    instrument: `str`
        The name of the instrument corresponding to the exposure.
    """
    global logger, config, source_butler, dest_butler, rucio_interface

    # Check several times (before each major step) for existence of the
    # result to avoid work in case of race conditions
    zip_name = f"{exp.obs_id}.zip"
    dest_dir = (
        ResourcePath(config.dest_uri_prefix).join(instrument).join(f"{exp.day_obs}")
    )
    dest_path = dest_dir.join(zip_name)
    if dest_path.exists():
        logger.info("Zip exists, skipping processing: %s", dest_path)
        return

    # Map exposure to tracts
    with source_butler.query() as q:
        q = q.join_dimensions(["tract"]).where(
            "visit = _exposure",
            instrument=instrument,
            skymap="lsst_cells_v1",
            bind={"_exposure": exp.id},
        )
        tracts = {id["tract"] for id in q.data_ids(["tract"])}

    # Find all datasets for this exposure and its source directory
    refs = source_butler.query_datasets(
        "raw",
        exposure=exp.id,
        instrument=instrument,
        collections=f"{instrument}/raw/all",
        explain=False,
    )
    if not refs:
        logger.warn("No datasets for exposure %s", exp.obs_id)
        return

    logger.info("Handling exposure: %s (%s)", exp.obs_id, len(refs))

    source_uri_dir = source_butler.getURI(refs[0]).dirname()
    logger.debug("Source directory: %s", source_uri_dir)

    expected_sensors_path = ResourcePath(source_uri_dir).join(
        f"{exp.obs_id}_expectedSensors.json"
    )
    if expected_sensors_path.exists():
        with expected_sensors_path.open("rb") as fd:
            expected_sensors = json.load(fd)["expectedSensors"]
        expected_refs = len([t for t in expected_sensors.values() if t == "SCIENCE"])
        if len(refs) < expected_refs:
            logger.warning(
                "Skipping incomplete exposure %s: %s < %s",
                exp.obs_id,
                len(refs),
                expected_refs,
            )
            return

    # Make a zip file for this exposure
    with tempfile.TemporaryDirectory() as tmpdir:
        prepdir = os.path.join(tmpdir, "inputs")
        os.mkdir(prepdir)
        os.chdir(prepdir)
        # Second race condition check
        if dest_path.exists():
            logger.info("Zip exists, not retrieving datasets: %s", dest_path)
            return

        # Get the raw datasets
        with time_this(logger, "Artifact retrieval"):
            logger.debug("Retrieving artifacts")
            retrieved = source_butler.retrieveArtifacts(
                refs, destination=prepdir, preserve_path=False
            )

        # Generate the index
        _index, okay, failed = index_files(
            [f.basename() for f in retrieved],
            "",
            -1,
            False,
            "metadata",
        )
        logger.debug("indexed")
        # ingest-raws needs to be changed to understand this change from
        # the default of _index.json.
        with open("_metadata_index.json", "w") as fd:
            json.dump(_index, fd)
        logger.debug("index written")

        # Transfer non-dataset files
        transfer_list = []
        for dirpath, dirnames, filenames in source_uri_dir.walk():
            for f in filenames:
                if not os.path.exists(f):
                    transfer_list.append((dirpath.join(f), ResourcePath(f)))
        # Third race condition check
        if dest_path.exists():
            logger.info("Zip exists, not copying others: %s", dest_path)
            return
        logger.debug("Also copying %s", [t[0] for t in transfer_list])
        ResourcePath.mtransfer("copy", transfer_list)

        # Zip everything.
        with time_this(logger, "Zip creation"):
            zip_path = os.path.join(tmpdir, zip_name)
            logger.debug("Writing to %s", zip_path)
            with zipfile.ZipFile(zip_path, "w") as zip_file:
                for f in os.listdir():
                    logger.debug("adding %s", f)
                    if f.endswith(".fits"):
                        zip_file.write(f, f, compress_type=zipfile.ZIP_STORED)
                    else:
                        zip_file.write(f, f, compress_type=zipfile.ZIP_DEFLATED)

        # Compute the Rucio hashes
        # We do this here rather than internally in RucioInterface so that we
        # can take advantage of the OS cache since we just wrote the file and
        # also so that we capture the state of the file just after creation,
        # in case the transfer to its final destination is corrupted.
        if config.rucio_rse:
            hashes = RucioInterface.compute_hashes(zip_path)

        # Fourth race condition check
        if dest_path.exists():
            logger.info("Zip exists, not installing: %s", dest_path)
            return
        # Copy to destination
        logger.info("Installing zip in %s", dest_path)
        with time_this(logger, "Installing zip"):
            if not config.dry_run:
                # The final race condition check is that transfer_from()
                # will not overwrite.
                dest_path.transfer_from(ResourcePath(zip_path), "copy")

        logger.debug("exporting dimensions")
        dimensions_file = os.path.join(tmpdir, "_dimensions.yaml")
        with source_butler.export(filename=dimensions_file) as export:
            export.saveDimensionData("exposure", [exp])
            dims = [
                "day_obs",
                "group",
                "visit",
                "visit_definition",
                "visit_detector_region",
                "visit_system",
                "visit_system_membership",
            ]
            for dim in dims:
                recs = source_butler.query_dimension_records(
                    dim,
                    exposure=exp.id,
                    limit=None,
                    instrument=instrument,
                    explain=False,
                )
                if recs:
                    logger.info("%s: %s", dim, recs)
                    export.saveDimensionData(dim, recs)
        dimensions_dest = dest_dir.join(f"{exp.obs_id}_dimensions.yaml")
        logger.info("Saving exported dimensions in %s", dimensions_dest)
        if not config.dry_run:
            dimensions_dest.transfer_from(ResourcePath(dimensions_file), "copy")
        if config.rucio_rse:
            dim_hashes = RucioInterface.compute_hashes(dimensions_file)

        # Done with tmpdir

    logger.info("Transferring dimension records to destination Butler repo")
    if not config.dry_run:
        dest_butler.transfer_dimension_records_from(source_butler, refs)

    logger.info("Ingesting zip: %s", dest_path)
    if not config.dry_run:
        with time_this(logger, "Ingesting zip"):
            dest_butler.ingest_zip(dest_path, transfer="direct")

    if config.rucio_rse:
        logger.info("Registering zip in Rucio")
        with time_this(logger, "Registering in Rucio"):
            rucio_interface.register(
                f"{instrument}/{exp.day_obs}/{zip_name}", hashes, tracts, config.dry_run
            )
            logger.info("Registering dimensions in Rucio")
            rucio_interface.register(
                f"{instrument}/{exp.day_obs}/{exp.obs_id}_dimensions.yaml",
                dim_hashes,
                tracts,
                config.dry_run,
            )


# Global variables

config: argparse.Namespace = None
logger: logging.Logger = None
source_butler: Butler = None
dest_butler: Butler = None
rucio_interface: RucioInterface = None


def initialize():
    """Set up the global variables."""
    global config, source_butler, dest_butler, logger, rucio_interface

    config = parse_args()

    CliLog.initLog(
        longlog=True, log_label={"LABEL": f"{config.now.isot}@{config.window}"}
    )
    levels = []
    if config.log:
        for keyval in config.log.split(","):
            key, _, val = keyval.rpartition("=")
            if key == "":
                key = None
            levels.append((key, val))
    CliLog.setLogLevels(logLevels=levels)
    logger = logging.getLogger("lsst.transfer.embargo.raw")
    logger.info("config: %s", config)

    source_butler = Butler(config.fromrepo, skymap="lsst_cells_v1")
    dest_butler = Butler(config.torepo, writeable=True)

    if config.rucio_rse:
        rucio_interface = RucioInterface(config.rucio_rse, config.scope)


def main():
    """Main function."""
    global config
    initialize()

    data_queries = []
    with open(config.config_file, "r") as f:
        for entry in yaml.safe_load(f):
            data_queries.append(DataQuery(**entry))
    logger.info("data_queries %s", data_queries)

    for data_query in data_queries:
        logger.info("Processing %s", data_query)
        transfer_data_query(data_query)


if __name__ == "__main__":
    main()
