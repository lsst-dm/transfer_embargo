import argparse

import astropy.time
from lsst.daf.butler import Butler, Timespan
from lsst.daf.butler.cli.cliLog import CliLog
import logging


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
        type=list,
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
    datasetTypeList = namespace.datasettype
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

    if namespace.log == "True":
        CliLog.initLog(longlog=True)
        logger = logging.getLogger("lsst.transfer.embargo")
        logger.info("from path: %s", namespace.fromrepo)
        logger.info("to path: %s", namespace.torepo)
    # the timespan object defines a "forbidden" region of time
    # starting at the nowtime minus the embargo period
    # and terminating in anything in the future
    # this forbidden timespan will be de-select
    # for moving any exposure that overlaps with it
    # documentation here:
    # https://community.lsst.org/t/constructing-a-where-for-query-dimension-records/6478
    timespan_embargo = Timespan(now - embargo_period, None)
    # The Dimensions query
    # If (now - embargo period, now) does not overlap
    # with observation time interval: move
    # Else: don't move
    # Save data Ids of these observations into a list
    datalist_exposure = []
    datalist_ingest = []
    for dtype in datasetTypeList:
        if any(
            dim in ["exposure", "visit"]
            for dim in [
                d.name for d in registry.queryDatasetTypes(datasetType)[0].dimensions
            ]
        ):
            datalist_exposure.append(dtype)
        else:
            datalist_ingest.append(dtype)
    print('datalist_exposure', datalist_exposure)
    print('datalist_ingest', datalist_ingest)
    
    # then, also move the appropriate collections
    # around according to how the datatypes moved 
    STOP
        
    
    # first, run all of the exposure types through
    outside_embargo = [
        dt.id
        for dt in registry.queryDimensionRecords(
            "exposure",
            dataId=dataId,
            datasets=datasetlist_exposure,
            collections=collections_exposure,
            where="NOT exposure.timespan OVERLAPS\
                                                    timespan_embargo",
            bind={"timespan_embargo": timespan_embargo},
        )
    ]
    # Query the DataIds after embargo period
    datasetRefs_exposure = registry.queryDatasets(
        datasetlist_exposure,
        dataId=dataId,
        collections=collections_exposure,
        where="exposure.id IN (exposure_ids)",
        bind={"exposure_ids": outside_embargo},
    ).expanded()

    if namespace.log == "True":
        ids_to_move = [dt.dataId.full["exposure"] for dt in datasetRefs_exposure]
        logger.info("exposure ids to move: %s", ids_to_move)
    out_exposure = dest.transfer_from(
        butler,
        source_refs=datasetRefs_exposure,
        transfer="copy",
        skip_missing=True,
        register_dataset_types=True,
        transfer_dimensions=True,
    )
    if namespace.log == "True":
        ids_moved = [
            dt.dataId.full["exposure"]
            for dt in scratch_registry.queryDatasets(
                datasetType=datasetlist_exposure, collections=collections_exposure
            )
        ]
        logger.info("exposure ids moved: %s", ids_moved)
    # now, time to move/copy all of the non-exposure
    # datasettypes
    datasetRefs_ingest = registry.queryDatasets(
        datasetType=datasetlist_ingest,
        collections=collections_ingest,
        where="ingest_date <= timespan_embargo_begin",
        bind={"timespan_embargo_begin": timespan_embargo.begin},
    )
    if namespace.log == "True":
        ids_to_move = [dt.id for dt in datasetRefs_ingest]
        logger.info("ingest ids to move: %s", ids_to_move)
    out_ingest = dest.transfer_from(
        butler,
        source_refs=datasetRefs_ingest,
        transfer="copy",
        skip_missing=True,
        register_dataset_types=True,
        transfer_dimensions=True,
    )
    if namespace.log == "True":
        ids_moved = [
            dt.id
            for dt in scratch_registry.queryDatasets(
                datasetType=datasetlist_ingest, collections=collections_ingest
            )
        ]
        logger.info("ingest ids moved: %s", ids_moved)

    if move == "True":
        # prune both datasettypes
        # is there a way to do this at the same time?
        butler.pruneDatasets(refs=datasetRefs_exposure, unstore=True, purge=True)
        butler.pruneDatasets(refs=datasetRefs_ingest, unstore=True, purge=True)
