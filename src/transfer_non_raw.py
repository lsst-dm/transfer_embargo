import argparse
import itertools
import logging
from collections.abc import Generator
from typing import Any

from astropy.time import Time, TimeDelta  # type: ignore
from lsst.daf.butler import (
    Butler,
    EmptyQueryResultError,
    Timespan,
)
from lsst.daf.butler.cli.cliLog import CliLog

from data_query import DataQuery


def _batched(items: list[Any], n: int) -> Generator:
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
    logger.info(f"Dataset types: {dataset_types}")

    end_time = config.now - TimeDelta(data_query.embargo_hours * 3600, format="sec")
    if config.window is not None:
        start_time = end_time - TimeDelta(config.window, format="quantity_str")
    else:
        start_time = Time(0, format="jd")
    ok_timespan = Timespan(start_time, end_time)

    for dataset_type in dataset_types:
        logger.info(f"Handling dataset type: {dataset_type}")
        if "visit" in dataset_type.dimensions:
            transfer_dimension("visit", dataset_type, data_query, ok_timespan)
        elif "exposure" in dataset_type.dimensions:
            transfer_dimension("exposure", dataset_type, data_query, ok_timespan)
        else:
            where = "(ingest_date overlaps _ok_timespan)"
            where += f" AND ({data_query.where})" if data_query.where else ""
            # data_query.where goes last to avoid injection overriding timespan
            transfer_dataset_type(
                dataset_type,
                data_query.collections,
                where,
                {"_ok_timespan": ok_timespan},
            )


def transfer_dimension(dimension, dataset_type, data_query, ok_timespan):
    global config, source_butler, logger
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
                explain=False,
            )
        ]
    except EmptyQueryResultError:
        logger.warning("No matching records for {dimension}")
        return
    logger.info("Got {len(ids)} dimension values for {dimension}")
    for id_batch in _batched(ids, 100):
        where = f"({dimension}.id IN (_ids))"
        where += f" AND ({data_query.where})" if data_query.where else ""
        # data_query.where goes last to avoid injection overriding id list
        transfer_dataset_type(
            dataset_type,
            data_query.collections,
            where,
            {"_ids": id_batch},
        )


def transfer_dataset_type(dataset_type, collections, where, bind):
    global source_butler, logger
    logger.debug(f"Querying datasets: {where} {bind}")
    dataset_refs = list(
        # ok to have empty results because this is used with batching.
        source_butler.query_datasets(
            dataset_type, collections, where=where, bind=bind, explain=False, limit=None
        )
    )
    logger.info(f"Got {len(dataset_refs)} datasets")
    for dsr_batch in _batched(dataset_refs, 1000):
        logger.debug("transfer_from(%s)", dataset_refs)
        if not config.dry_run:
            dest_butler.transfer_from(
                source_butler,
                dataset_refs,
                transfer="copy",
                skip_missing=True,
                register_dataset_types=True,
                transfer_dimensions=True,
            )


config: argparse.Namespace
logger: logging.Logger
source_butler: Butler
dest_butler: Butler


def initialize():
    global config, source_butler, dest_butler, logger

    config = parse_args()

    # Initialize the logger and set the level
    CliLog.initLog(longlog=True)
    CliLog.setLogLevels(logLevels=[(None, config.log)])
    logger = logging.getLogger("lsst.transfer.embargo")
    logger.info("log level %s", config.log)
    logger.info("config: %s", config)
    if config.dry_run:
        logger.warning("dry run; no transfers will occur")

    # Define embargo and destination butler
    source_butler = Butler(config.fromrepo, instrument=config.instrument)
    dest_butler = Butler(config.torepo, writeable=True)


def main():
    global config, logger

    initialize()

    if config.config_file:
        logger.info("using config file %s", config.config_file)
        with open(config.config_file, "r") as f:
            data_queries = DataQuery.from_yaml(f)
    else:
        logger.info("Using dataqueries: %s", config.dataqueries)
        data_queries = DataQuery.from_yaml(config.dataqueries)
    logger.info("data_queries %s", data_queries)

    for data_query in data_queries:
        logger.info("Processing %s", data_query)
        transfer_data_query(data_query)
    return 0


if __name__ == "__main__":
    main()
