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
import logging
import random
import time
from typing import Any

import sqlalchemy
from lsst.daf.butler import Butler, CollectionType, DatasetType
from lsst.daf.butler.cli.cliLog import CliLog


def parse_args():
    """Parses and returns command-line arguments.

    Returns
    -------
    ns : argparse.Namespace
        An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Get all datasets in a collection.")
    parser.add_argument(
        "repo",
        type=str,
        help="Butler repository to query.",
    )
    parser.add_argument(
        "collection",
        type=str,
        help="Collection to query.",
    )
    parser.add_argument(
        "--restart",
        type=str,
        required=False,
        default=None,
        help="Dataset type to restart at.",
    )

    parser.add_argument(
        "--log",
        type=str,
        required=False,
        default="INFO",
        help="Default is INFO level, other options are DEBUG or WARNING.",
    )

    ns = parser.parse_args()
    return ns


def dbretry(retry_label: str, func: Any, *args, **kwargs) -> Any:
    """Retry a database-dependent function call up to 10 times."""
    global logger

    retries = 0
    max_retries = 10
    while retries < max_retries:
        try:
            return func(*args, **kwargs)
        except (sqlalchemy.exc.InterfaceError, sqlalchemy.exc.OperationalError) as e:
            retries += 1
            logger.warning(f"{retry_label} retry {retries}: {e}")
            time.sleep(random.uniform(2, 10))
    if retries >= max_retries:
        raise RuntimeError("Unable to communicate with database")


def gather_collection_info(collection: str):
    """Get information for a collection and its children."""
    global logger, butler

    collection_info = butler.collections.get_info(collection)
    calibration_collections = set()
    tagged_collections = set()
    if collection_info.type == CollectionType.CHAINED:
        logger.info(f"Chained collection {collection}")
        for child in collection_info.children:
            calibs, tagged = gather_collection_info(child)
            calibration_collections |= calibs
            tagged_collections |= tagged
    elif collection_info.type == CollectionType.TAGGED:
        logger.info(f"Tagged collection {collection}")
        tagged_collections.add(collection)
    elif collection_info.type == CollectionType.CALIBRATION:
        logger.info(f"Calibration collection {collection}")
        calibration_collections.add(collection)
    elif collection_info.type == CollectionType.RUN:
        # Treat certain RUN collections like CALIBRATION
        if (
            "/calib/" in collection
            or collection.startswith("pretrained_models/")
            or collection.startswith("refcats/")
            or collection == "skymaps"
        ):
            logger.info(f"Run/Calibration collection {collection}")
            calibration_collections.add(collection)
    return calibration_collections, tagged_collections


def generate_dstypes(collection: str) -> set[DatasetType]:
    """Generate a list of exportable dataset types from a collection."""
    global butler, logger

    all_types = butler.registry.queryDatasetTypes("*")
    collection_info = butler.collections.query_info(collection, include_summary=True)
    calibration_collections, tagged_collections = gather_collection_info(collection)
    logger.info(f"{calibration_collections=}")
    logger.info(f"{tagged_collections=}")

    # Get only datasets types that are in the collection
    dataset_type_names = set(
        butler.collections._filter_dataset_types(
            [d.name for d in all_types], collection_info
        )
    )
    logger.info(f"{len(dataset_type_names)} dataset types")

    # Remove raw dataset types
    dataset_type_names -= {"raw", "guider_raw"}

    # Remove any dataset types found in calibration collections
    calib_dataset_type_names = set()
    for calib in calibration_collections:
        calib_info = butler.collections.query_info(calib, include_summary=True)
        calib_dataset_type_names |= set(
            butler.collections._filter_dataset_types(dataset_type_names, calib_info)
        )
    logger.info(f"Removing {len(calib_dataset_type_names)}: {calib_dataset_type_names}")
    dataset_type_names -= calib_dataset_type_names

    dataset_types = {d for d in all_types if d.name in dataset_type_names}
    num_dataset_types = len(dataset_types)
    if len(dataset_type_names) != num_dataset_types:
        raise RuntimeError(
            "Mismatch between {len(dataset_type_names)} names"
            " and {num_dataset_types} types"
        )
    logger.info(f"Dataset types ({num_dataset_types}): {sorted(dataset_type_names)}")
    return dataset_types


def generate_datasets(
    collection: str, dataset_types: set[DatasetType], restart: str | None = None
):
    """Generate a list of datasets of particular types from a collection."""
    total_refs = 0
    num_dataset_types = len(dataset_types)
    for i, dataset_type in enumerate(sorted(dataset_types)):
        if restart is not None and dataset_type.name < restart:
            continue
        refs = dbretry(
            f"{dataset_type}: {i}/{num_dataset_types}",
            butler.query_datasets,
            dataset_type,
            collections=collection,
            find_first=True,
            limit=None,
            explain=False,
        )
        n_refs = 0
        for ref in refs:
            n_refs += 1
            print(ref.to_json())
        logger.info(f"{dataset_type}: {n_refs} refs")
        total_refs += n_refs
    logger.info(f"Total refs: {total_refs}")


logger: logging.Logger
butler: Butler


def initialize():
    global butler, logger

    config = parse_args()

    # Initialize the logger and set the level
    CliLog.initLog(longlog=True)
    CliLog.setLogLevels(logLevels=[(None, config.log)])
    logger = logging.getLogger("lsst.transfer.embargo.generate")
    logger.info("log level %s", config.log)
    logger.info("config: %s", config)

    # Define embargo and destination butler
    butler = Butler(config.repo)
    return config


def main():
    config = initialize()
    dstypes = generate_dstypes(config.collection)
    generate_datasets(config.collection, dstypes, config.restart)


if __name__ == "__main__":
    main()
