import argparse

import astropy.time
from lsst.daf.butler import Butler, Timespan
from lsst.daf.butler.cli.cliLog import CliLog


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transferring data from embargo butler to another butler"
    )

    # at least one arg in dataId needed for 'where' clause.
    parser.add_argument(
        "fromrepo",
        type=str,
        nargs="?",
        default="/repo/embargo",
        help="Butler Repository path from which data is transferred. Input str. Default = '/repo/embargo'",
    )
    parser.add_argument(
        "torepo",
        type=str,
        help="Repository to which data is transferred. Input str",
    )
    parser.add_argument(
        "instrument",
        type=str,
        nargs="?",
        default="LATISS",
        help="Instrument. Input str",
    )
    parser.add_argument(
        "--embargohours",
        type=float,
        required=False,
        default=80.0,
        help="Embargo time period in hours. Input float",
    )
    parser.add_argument(
        "--datasettype",
        type=str,
        required=False,
        default="raw",
        help="Dataset type. Input str",
    )
    parser.add_argument(
        "--collections",
        type=str,
        required=False,
        default="LATISS/raw/all",
        help="Data Collections. Input str",
    )
    parser.add_argument(
        "--nowtime",
        type=str,
        required=False,
        default="now",
        help="Now time in (ISO, TAI timescale). If left blank it will \
                        use astropy.time.Time.now.",
    )
    parser.add_argument(
        "--move",
        type=str,
        required=False,
        default="False",
        help="Copies if False, deletes original if True",
    )
    parser.add_argument(
        "--log",
        type=str,
        required=False,
        default="False",
        help="No logging if False, longlog if True",
    )
    return parser.parse_args()


if __name__ == "__main__":
    namespace = parse_args()
    # Define embargo and destination butler
    # If move is true, then you'll need write
    # permissions from the fromrepo (embargo)
    butler = Butler(namespace.fromrepo, writeable=namespace.move)
    registry = butler.registry
    dest = Butler(namespace.torepo, writeable=True)
    scratch_registry = dest.registry
    datasetType = namespace.datasettype
    collections = namespace.collections
    move = namespace.move
    # Dataset to move
    dataId = {"instrument": namespace.instrument}
    # Define embargo period
    embargo_period = astropy.time.TimeDelta(
        namespace.embargohours * 3600.0, format="sec"
    )
    if namespace.nowtime != "now":
        now = astropy.time.Time(namespace.nowtime, scale="tai", format="iso")
    else:
        now = astropy.time.Time.now().tai
    timespan_embargo = Timespan(now - embargo_period, now)
    # The Dimensions query
    # If (now - embargo period, now) does not overlap
    # with observation time interval: move
    # Else: don't move
    # Save data Ids of these observations into a list
    after_embargo = [
        dt.id
        for dt in registry.queryDimensionRecords(
            "exposure",
            dataId=dataId,
            datasets=datasetType,
            collections=collections,
            where="NOT exposure.timespan OVERLAPS\
                                                    timespan_embargo",
            bind={"timespan_embargo": timespan_embargo},
        )
    ]
    # Query the DataIds after embargo period
    datasetRefs = registry.queryDatasets(
        datasetType,
        dataId=dataId,
        collections=collections,
        where="exposure.id IN (exposure_ids)",
        bind={"exposure_ids": after_embargo},
    ).expanded()
    print("dataid in butler:", [dt.dataId.full["exposure"] for dt in datasetRefs])
    if namespace.log:
        cli_log = CliLog.initLog(longlog=True)
        CliLog.setLogLevels([("", "VERBOSE")])
    out = dest.transfer_from(
        butler,
        source_refs=datasetRefs,
        transfer="copy",
        skip_missing=True,
        register_dataset_types=True,
        transfer_dimensions=True,
    )
    print("out from transfer_from", out)
    print(
        "dataid in dest:",
        [
            dt.dataId.full["exposure"]
            for dt in scratch_registry.queryDatasets(datasetType=..., collections=...)
        ],
    )
    if move == "True":
        butler.pruneDatasets(refs=datasetRefs, unstore=True, purge=True)
