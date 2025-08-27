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
import io
import logging
import random
import sys
import time
from collections.abc import Generator
from typing import Any

import sqlalchemy
from lsst.daf.butler import Butler, DatasetRef, DimensionUniverse
from lsst.daf.butler.cli.cliLog import CliLog


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check dataset registration or existence."
    )
    parser.add_argument(
        "--butler",
        required=False,
        default="/repo/main",
        type=str,
        help="Source Butler repo (default=/repo/main).",
    )
    parser.add_argument(
        "infile",
        nargs="?",
        type=argparse.FileType("r"),
        default=sys.stdin,
    )
    parser.add_argument(
        "--exists",
        action="store_true",
    )
    parser.add_argument(
        "--restart",
        required=False,
        default=None,
        type=int,
        help="Batch number to restart at.",
    )
    parser.add_argument(
        "--batch",
        required=False,
        default=1000,
        type=int,
        help="Batch size (default=1000).",
    )
    parser.add_argument(
        "--log",
        type=str,
        required=False,
        default="INFO",
        help="Log level (default=INFO).",
    )

    ns = parser.parse_args()
    return ns


def batched(generator: Generator, n: int) -> Generator:
    """Return batches of items as lists."""
    batch = []
    for dsr in generator:
        batch.append(dsr)
        if len(batch) >= n:
            yield batch
            batch = []
    yield batch


def read_dsrs(fd: io.TextIOBase, dimensions: DimensionUniverse) -> DatasetRef:
    """Read DatasetRefs in JSON format."""
    for dsr_json in fd:
        dsr = DatasetRef.from_json(dsr_json, universe=dimensions)
        yield dsr


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


logger: logging.Logger


def main():
    global logger
    config = parse_args()

    CliLog.initLog(longlog=True)
    CliLog.setLogLevels(logLevels=[(None, config.log)])
    logger = logging.getLogger("lsst.transfer.embargo.check_non_raw")
    logger.info("config: %s", config)

    butler = Butler(config.butler)
    i = 0
    for batch in batched(read_dsrs(config.infile, butler.dimensions), config.batch):
        i += 1
        if config.restart is not None and i < config.restart:
            continue
        logger.info(f"Processing batch {i}")
        if config.exists:
            result = dbretry(f"Batch {i}", butler._datastore.mexists, batch)
        else:
            result = dbretry(f"Batch {i}", butler._datastore.knows_these, batch)
        for r in result:
            print(result[r], r.to_json())


if __name__ == "__main__":
    main()
