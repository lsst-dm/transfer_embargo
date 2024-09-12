import argparse
import itertools
import logging
import os
import tempfile
import time
import yaml
import zlib
from dataclasses import dataclass
from typing import Any

import pydantic
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


def _batched(l: list[Any], n: int) -> list[Any]:
    iterator = iter(l)
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
        default="now",
        type=str,
        help=(
            "Now time in (ISO, TAI timescale)."
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
        "--log",
        type=str,
        required=False,
        default="INFO",
        help="Default is INFO level, other options are DEBUG or WARNING.",
    )

    ns = parser.parse_args()
    ns.now = Time(ns.now, format="isot", scale="utc") if ns.now else Time.now()
    if ns.now > Time.now():
        raise ValueError(f"--now is in the future: {ns.now}")
    return ns


def transfer_data_query(data_query):
    global config, source_butler, dest_butler

    dataset_types = source_butler.registry.queryDatasetTypes(data_query.dataset_types)
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
        start_time = Time(0)
    ok_timespan = Timespan(start_time, end_time)

    for dataset_type in dataset_types:
        if "visit" in dataset_type.dimensions:
            transfer_dimension("visit", dataset_type, data_query, ok_timespan)
        elif "exposure" in dataset_type.dimensions:
            transfer_dimension("exposure", dataset_type, data_query, ok_timespan)
        else:
            # data_query.where goes last to avoid injection overriding timespan
            transfer_dataset_type(
                dataset_type,
                data_query.collections,
                f"(ingest_date in _ok_timespan) AND ({data_query.where})",
                {"_ok_timespan": ok_timespan},
                data_query.is_raw,
            )


def transfer_dimension(dimension, dataset_type, data_query, ok_timespan):
    global config, source_butler
    try:
        # data_query.where goes last to avoid injection overriding timespan
        dim_where = f"({dimension}.timespan OVERLAPS _ok_timespan) AND ({data_query.where})"
        dim_bind = {"_ok_timespan": ok_timespan}
        logger.info("Querying dimension %s: %s %s", dimension, dim_where, dim_bind)
        ids = [r.id for r in source_butler.query_dimension_records(
            dimension,
            instrument=config.instrument,
            where=dim_where,
            bind=dim_bind,
            limit=None,
        )]
    except EmptyQueryResultError:
        logger.warning("No matching records for %s", dimension)
        return
    for id_batch in _batched(ids, 100):
        # data_query.where goes last to avoid injection overriding id list
        transfer_dataset_type(
            dataset_type,
            data_query.collections,
            f"({dimension}.id IN (_ids)) AND ({data_query.where})",
            {"_ids": id_batch},
            data_query.is_raw,
        )

def transfer_dataset_type(dataset_type, collections, where, bind, is_raw):
    global source_butler
    logger.info("Querying datasets: %s %s", where, bind)
    dataset_refs = list(source_butler.query_datasets(
        dataset_type, collections, where=where, bind=bind
    ))
    for dsr_batch in _batched(dataset_refs, 1000):
        transfer_datasets(dsr_batch, is_raw)


def transfer_datasets(dataset_refs: list[DatasetRef], is_raw):
    global config, source_butler, dest_butler
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


config: argparse.Namespace = None
logger: logging.Logger = None
source_butler: Butler = None
dest_butler: Butler = None


def initialize():
    global config, source_butler, dest_butler, logger

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
