import argparse
import hashlib
import itertools
import logging
import random
import re
import time
import yaml
import zlib
from typing import Any

import pydantic
import rucio.common.exception
from astropy.time import Time, TimeDelta
from lsst.daf.butler import (
    Butler,
    CollectionType,
    DatasetRef,
    EmptyQueryResultError,
    FileDataset,
    Timespan,
)
from lsst.daf.butler.cli.cliLog import CliLog
from lsst.resources import ResourcePath
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient


class DataQuery(pydantic.BaseModel):
    collections: str | list[str]
    """Collection names or glob patterns to search."""

    dataset_types: str | list[str]
    """List of dataset types or glob patterns to transfer."""

    where: str
    """Where clause expression to select datasets to transfer."""

    embargo_hours: float
    """How long to embargo the selected datasets (hours)."""

    is_raw: bool
    """Treat the matching datasets as raw (archival) data."""


def from_yaml(yaml_source: Any) -> list[DataQuery]:
    result = []
    for entry in yaml.safe_load(yaml_source):
        result.append(DataQuery(**entry))
    return result


class RucioInterface:
    """Register files in Rucio and attach them to datasets.

    Parameters
    ----------
    rucio_rse: `str`
        Name of the RSE that the files live in.
    dtn_url: `str`
        Base URL of the data transfer node for the Rucio physical filename.
    scope: `str`
        Rucio scope to register the files in.
    root: 'str'
        Root URL for direct-ingested raw files.
    """

    def __init__(self, rucio_rse: str, dtn_url: str, scope: str, root: str):
        self.rucio_rse = rucio_rse
        if not dtn_url.endswith("/"):
            dtn_url += "/"
        self.pfn_base = dtn_url
        self.scope = scope
        self.root = ResourcePath(root)

        self.replica_client = ReplicaClient()
        self.did_client = DIDClient()

    def _make_did(self, dataset_ref: DatasetRef, path: str) -> dict[str, str | int]:
        """Make a Rucio data identifier dictionary from a resource.

        Parameters
        ----------
        path: `str`
            Root-relative path to the file.

        Returns
        -------
        did: `dict [ str, str|int ]`
            Rucio data identifier including physical and logical names,
            byte length, adler32 and MD5 checksums, and scope.
        """
        location = self.root.join(path)
        with location.open("rb") as f:
            contents = f.read()
            size = len(contents)
            md5 = hashlib.md5(contents).hexdigest()
            adler32 = f"{zlib.adler32(contents):08x}"
        path = path.removeprefix("/")
        pfn = self.pfn_base + path
        meta = dict(rubin_butler=1, rubin_sidecar=dataset_ref.to_json())
        return dict(
            pfn=pfn,
            bytes=size,
            adler32=adler32,
            md5=md5,
            name=path,
            scope=self.scope,
            meta=meta,
        )

    def _add_files_to_dataset(self, dids: list[dict], dataset_id: str) -> None:
        """Attach a list of files specified by Rucio DIDs to a Rucio dataset.

        Ignores already-attached files for idempotency.

        Parameters
        ----------
        dids: `list [ dict [ str, str|int ] ]`
            List of Rucio data identifiers.
        dataset_id: `str`
            Logical name of the Rucio dataset.
        """
        retries = 0
        max_retries = 2
        while True:
            try:
                self.did_client.add_files_to_dataset(
                    scope=self.scope,
                    name=dataset_id,
                    files=dids,
                    rse=self.rucio_rse,
                )
                return
            except rucio.common.exception.FileAlreadyExists:
                # At least one already is in the dataset.
                # This shouldn't happen, but if it does,
                # we have to retry each individually.
                for did in dids:
                    try:
                        self.did_client.add_files_to_dataset(
                            scope=self.scope,
                            name=dataset_id,
                            files=[did],
                            rse=self.rucio_rse,
                        )
                    except rucio.common.exception.FileAlreadyExists:
                        pass
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
        self, dataset_refs: list[DatasetRef], paths: list[str], dry_run: bool = False
    ) -> None:
        """Register a list of files in Rucio.

        Parameters
        ----------
        dataset_refs: `list` [ `DatasetRef` ]
            List of Butler source repo dataset refs.
        paths: `list` [ `str` ]
            List of relative paths to files relative to direct root.
        dry_run: `bool`
            Do not take any actions if true.
        """
        data = [self._make_did(dsr, p) for dsr, p in zip(dataset_refs, paths)]
        datasets = dict()
        for did in data:
            # For non-science images, use a dataset per 100 exposures
            dataset_id = re.sub(
                r"^(.+?)/(\d{8})/[A-Z]{2}_[A-Z]_\2_(\d{4})\d{2}/.*",
                r"Dataset/\1/\2/\3",
                did["name"],
            )
            datasets.setdefault(dataset_id, []).append(did)
            # TODO: compute datasets for science images based on visit/tract

        for dataset_id, dids in datasets.items():
            try:
                logger.info(
                    "Registering %s in dataset %s, RSE %s",
                    dids,
                    dataset_id,
                    self.rucio_rse,
                )
                if not dry_run:
                    self._add_files_to_dataset(dids, dataset_id)
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
                self._add_files_to_dataset(dids, dataset_id)

        logger.info("Done with Rucio for %s", paths)


def _batched(items: list[Any], n: int) -> list[Any]:
    iterator = iter(items)
    while batch := list(itertools.islice(iterator, n)):
        yield batch


def parse_args():
    """Parses and returns command-line arguments
    for transferring data between Butler repositories.

    This function sets up an argument parser for transferring
    data from an embargo Butler repository to another Butler repository.
    It defines several arguments and their options, including source
    and destination repositories.

    Returns
    -------
    ns : argparse.Namespace
        An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Transferring data from embargo butler to another butler."
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
        "instrument",
        type=str,
        help="Name of instrument to transfer datasets for.",
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

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--config_file",
        required=False,
        type=str,
        help="Path to YAML config file, if used.",
    )
    group.add_argument(
        "--dataqueries",
        required=False,
        type=str,
        help="A YAML string representation of data queries.",
    )

    parser.add_argument(
        "--dest_uri_prefix",
        type=str,
        required=False,
        help="Destination URI prefix for raw data.",
    )

    parser.add_argument(
        "--rucio_rse",
        type=str,
        required=False,
        help="Rucio RSE for raw data.",
    )
    parser.add_argument(
        "--dtn_url",
        type=str,
        required=False,
        help="DTN URL for Rucio access to raw data.",
    )
    parser.add_argument(
        "--scope",
        type=str,
        required=False,
        help="Rucio scope for raw data.",
    )

    parser.add_argument(
        "--log",
        type=str,
        required=False,
        default="INFO",
        help="Default is INFO level, other options are DEBUG or WARNING.",
    )

    ns = parser.parse_args()
    ns.now = Time(ns.now, format="isot", scale="tai") if ns.now else Time.now()
    if ns.now > Time.now():
        raise ValueError(f"--now is in the future: {ns.now}")
    if ns.rucio_rse is not None:
        if ns.dest_uri_prefix is None:
            raise ValueError("--dest_uri_prefix required with --rucio_rse")
        if ns.dtn_url is None:
            raise ValueError("--dtn_url required with --rucio_rse")
        if ns.scope is None:
            raise ValueError("--scope required with --rucio_rse")

    return ns


def transfer_data_query(data_query):
    global config, source_butler, dest_butler

    all_types = source_butler.registry.queryDatasetTypes(data_query.dataset_types)
    collections_info = source_butler.collections.query_info(
        data_query.collections, include_summary=True
    )
    dataset_type_names = source_butler.collections._filter_dataset_types(
        [d.name for d in all_types], collections_info
    )
    dataset_types = {d for d in all_types if d.name in dataset_type_names}
    if data_query.is_raw:
        for dataset_type in dataset_types:
            logger.info("registerDatasetType(%s)", dataset_type)
            if not config.dry_run:
                dest_butler.registry.registerDatasetType(dataset_type)
        collections = source_butler.collections.query(
            data_query.collections, CollectionType.RUN
        )
        for collection in collections:
            logger.info("register run collection %s", collection)
            if not config.dry_run:
                dest_butler.collections.register(collection)

    end_time = config.now - TimeDelta(data_query.embargo_hours * 3600, format="sec")
    if config.window is not None:
        start_time = end_time - TimeDelta(config.window, format="quantity_str")
    else:
        start_time = Time(0, format="jd")
    ok_timespan = Timespan(start_time, end_time)

    for dataset_type in dataset_types:
        if "visit" in dataset_type.dimensions:
            transfer_dimension("visit", dataset_type, data_query, ok_timespan)
        elif "exposure" in dataset_type.dimensions:
            transfer_dimension("exposure", dataset_type, data_query, ok_timespan)
        else:
            where = "(ingest_date in _ok_timespan)"
            where += f" AND ({data_query.where})" if data_query.where else ""
            # data_query.where goes last to avoid injection overriding timespan
            transfer_dataset_type(
                dataset_type,
                data_query.collections,
                where,
                {"_ok_timespan": ok_timespan},
                data_query.is_raw,
            )


def transfer_dimension(dimension, dataset_type, data_query, ok_timespan):
    global config, source_butler
    try:
        # data_query.where goes last to avoid injection overriding timespan
        dim_where = f"({dimension}.timespan OVERLAPS _ok_timespan)"
        dim_where += f" AND ({data_query.where})" if data_query.where else ""
        dim_bind = {"_ok_timespan": ok_timespan}
        logger.info("Querying dimension %s: %s %s", dimension, dim_where, dim_bind)
        ids = [
            r.id
            for r in source_butler.query_dimension_records(
                dimension,
                instrument=config.instrument,
                where=dim_where,
                bind=dim_bind,
                limit=None,
            )
        ]
    except EmptyQueryResultError:
        logger.warning("No matching records for %s", dimension)
        return
    for id_batch in _batched(ids, 100):
        where = f"({dimension}.id IN (_ids))"
        where += f" AND ({data_query.where})" if data_query.where else ""
        # data_query.where goes last to avoid injection overriding id list
        transfer_dataset_type(
            dataset_type,
            data_query.collections,
            where,
            {"_ids": id_batch},
            data_query.is_raw,
        )


def transfer_dataset_type(dataset_type, collections, where, bind, is_raw):
    global source_butler
    logger.info("Querying datasets: %s %s", where, bind)
    dataset_refs = list(
        # ok to have empty results because this is used with batching.
        source_butler.query_datasets(
            dataset_type, collections, where=where, bind=bind, explain=False, limit=None
        )
    )
    for dsr_batch in _batched(dataset_refs, 1000):
        transfer_datasets(dsr_batch, is_raw)


def transfer_datasets(dataset_refs: list[DatasetRef], is_raw):
    global config, source_butler, dest_butler, rucio_interface
    if not is_raw:
        logger.info("transfer_from(%s)", dataset_refs)
        if not config.dry_run:
            dest_butler.transfer_from(
                source_butler,
                dataset_refs,
                transfer="copy",
                skip_missing=True,
                register_dataset_types=True,
                transfer_dimensions=True,
            )
    else:
        dest_root = ResourcePath(config.dest_uri_prefix)
        filedatasets = []
        relative_rps = []
        for dataset_ref in dataset_refs:
            source_rp = source_butler.getURI(dataset_ref)
            # Assumes that source is coming from an s3://bucket/path URL.
            relative_rp = source_rp.relativeToPathRoot
            dest_rp = dest_root.join(relative_rp)
            if dest_rp.exists():
                logger.info("Direct destination exists, not transferring: %s", dest_rp)
            else:
                logger.info("Direct transfer: %s to %s", source_rp, dest_rp)
                if not config.dry_run:
                    dest_rp.transfer_from(source_rp, transfer="copy")
            filedatasets.append(FileDataset(dest_rp, dataset_ref))
            relative_rps.append(relative_rp)
        logger.info("transfer_dimension_records(%s)", dataset_refs)
        if not config.dry_run:
            dest_butler.transfer_dimension_records_from(source_butler, dataset_refs)
        logger.info("ingest(%s)", filedatasets)
        if not config.dry_run:
            dest_butler.ingest(*filedatasets, transfer="direct")
        if config.rucio_rse:
            rucio_interface.register(dataset_refs, relative_rps, config.dry_run)


config: argparse.Namespace = None
logger: logging.Logger = None
source_butler: Butler = None
dest_butler: Butler = None
rucio_interface: RucioInterface = None


def initialize():
    global config, source_butler, dest_butler, logger, rucio_interface

    config = parse_args()

    # Initialize the logger and set the level
    CliLog.initLog(longlog=True)
    CliLog.setLogLevels(logLevels=[(None, config.log)])
    logger = logging.getLogger("lsst.transfer.embargo")
    logger.info("log level %s", config.log)
    logger.info("config: %s", config)

    # Define embargo and destination butler
    source_butler = Butler(config.fromrepo, instrument=config.instrument)
    dest_butler = Butler(config.torepo, writeable=True)

    if config.rucio_rse:
        if config.dry_run:
            rucio_interface = RucioInterface(
                config.rucio_rse,
                config.dtn_url,
                config.scope,
                "s3://embargo@rubin-summit/",
            )
        else:
            rucio_interface = RucioInterface(
                config.rucio_rse,
                config.dtn_url,
                config.scope,
                config.dest_uri_prefix,
            )


def main():
    global config

    initialize()

    if config.config_file:
        logger.info("using config file %s", config.config_file)
        with open(config.config_file, "r") as f:
            data_queries = from_yaml(f)
    else:
        logger.info("Using dataqueries: %s", config.dataqueries)
        data_queries = from_yaml(config.dataqueries)
    logger.info("data_queries %s", data_queries)

    for data_query in data_queries:
        if data_query.is_raw and config.dest_uri_prefix is None:
            raise argparse.ArgumentError("--dest_uri_prefix required for raw data")

    for data_query in data_queries:
        logger.info("Processing %s", data_query)
        transfer_data_query(data_query)
    return 0


if __name__ == "__main__":
    main()
