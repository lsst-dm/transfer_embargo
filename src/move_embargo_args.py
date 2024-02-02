import argparse

import astropy.time
from lsst.daf.butler import Butler, Timespan
from lsst.daf.butler.cli.cliLog import CliLog
import logging

import lsst
import os


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
        required=False,
        nargs="+",
        # default=[]
        help="Dataset type. Input list or str",
    )
    parser.add_argument(
        "--collections",
        # type=str,
        nargs="+",
        required=False,
        default="LATISS/raw/all",
        help="Data Collections. Input list or str",
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
    parser.add_argument(
        "--desturiprefix",
        type=str,
        required=False,
        default="False",
        help="Define dest uri if you need to run ingest for raws",
    )
    return parser.parse_args()


if __name__ == "__main__":
    namespace = parse_args()
    # Define embargo and destination butler
    # If move is true, then you'll need write
    # permissions from the fromrepo (embargo)
    butler = Butler(namespace.fromrepo, writeable=namespace.move)
    registry = butler.registry
    dest_butler = Butler(namespace.torepo, writeable=True)
    dest_registry = dest_butler.registry
    datasetTypeList = namespace.datasettype
    print('whats the datasettypelist in here', datasetTypeList)
    collections = namespace.collections
    move = namespace.move
    dest_uri_prefix = namespace.desturiprefix
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
    collections_exposure = []
    datalist_no_exposure = []
    collections_no_exposure = []

    for i, dtype in enumerate(datasetTypeList):
        if any(
            dim in ["exposure", "visit"]
            for dim in registry.queryDatasetTypes(dtype)[0].dimensions.names
        ):
            datalist_exposure.append(dtype)
            collections_exposure.append(collections[i])
        else:
            # these should be the raw datasettype
            datalist_no_exposure.append(dtype)
            collections_no_exposure.append(collections[i])

    # sort out which dtype goes into which list
    if namespace.log == "True":
        logger.info("datalist_exposure to move: %s", datalist_exposure)
        logger.info("datalist_no_exposure to move: %s", datalist_no_exposure)

    # because some dtypes don't have an exposure dimension
    # we will need a different option to move those
    # ie deepcoadds
    # but first we will move all dtypes that have an
    # exposure or visit dimension (ie calexp and raw)

    if datalist_exposure:  # if there is anything in the list
        # first, run all of the exposure types through
        outside_embargo = [
            dt.id
            for dt in registry.queryDimensionRecords(
                "exposure",
                dataId=dataId,
                datasets=datalist_exposure,
                collections=collections_exposure,
                where="NOT exposure.timespan OVERLAPS\
                                                        timespan_embargo",
                bind={"timespan_embargo": timespan_embargo},
            )
        ]
        # Query the DataIds after embargo period
        datasetRefs_exposure = registry.queryDatasets(
            datalist_exposure,
            dataId=dataId,
            collections=collections_exposure,
            where="exposure.id IN (exposure_ids)",
            bind={"exposure_ids": outside_embargo},
        ).expanded()
        

        if namespace.log == "True":
            ids_to_move = [dt.dataId.mapping["exposure"] for dt in datasetRefs_exposure]
            logger.info("exposure ids to move: %s", ids_to_move)

        # raw dtype requires special handling for the transfer,
        # so separate by dtype:
        for dtype in datalist_exposure:
            if dtype == "raw":
                # first check that the destination uri is defined
                assert (
                    dest_uri_prefix
                ), f"dest_uri_prefix needs to be specified to transfer raw datatype, {dest_uri_prefix}"
                # define a new filedataset_list using URIs
                dest_uri = lsst.resources.ResourcePath(dest_uri_prefix)
                source_uri = butler.get_many_uris(datasetRefs_exposure)
                filedataset_list = []
                for key, value in source_uri.items():
                    source_path_uri = value[0]
                    source_path = source_path_uri.relative_to(value[0].root_uri())
                    new_dest_uri = dest_uri.join(source_path)
                    if os.path.exists(source_path):
                        if namespace.log == "True":
                            logger.info("source path uri already exists")
                    else:
                        new_dest_uri.transfer_from(source_path_uri, transfer="copy")
                    filedataset_list.append(
                        lsst.daf.butler.FileDataset(new_dest_uri, key)
                    )

                # register datasettype and collection run only once
                try:
                    dest_butler.registry.registerDatasetType(
                        list(datasetRefs_exposure)[0].datasetType
                    )
                    dest_butler.registry.registerRun(list(datasetRefs_exposure)[0].run)

                    # ingest to the destination butler
                    dest_butler.transfer_dimension_records_from(
                        butler, datasetRefs_exposure
                    )
                    dest_butler.ingest(*filedataset_list, transfer="direct")
                except IndexError:
                    # this will be thrown if nothing is being moved
                    if namespace.log == "True":
                        logger.info("nothing in datasetRefs_exposure")

            else:
                dest_butler.transfer_from(
                    butler,
                    source_refs=datasetRefs_exposure,
                    transfer="copy",
                    skip_missing=True,
                    register_dataset_types=True,
                    transfer_dimensions=True,
                )
        if namespace.log == "True":
            ids_moved = [
                dt.dataId.mapping["exposure"]
                for dt in dest_registry.queryDatasets(
                    datasetType=datalist_exposure, collections=collections_exposure
                )
            ]
            logger.info("exposure ids moved: %s", ids_moved)
    if datalist_no_exposure:
        # this is for datatypes that don't have an exposure
        # or visit dimension
        # ie deepcoadds need to be queried using an ingest
        # date keyword
        datasetRefs_no_exposure = registry.queryDatasets(
            datasetType=datalist_no_exposure,
            collections=collections_no_exposure,
            where="ingest_date <= timespan_embargo_begin",
            bind={"timespan_embargo_begin": timespan_embargo.begin},
        )
        if namespace.log == "True":
            ids_to_move = [dt.id for dt in datasetRefs_no_exposure]
            logger.info("ingest ids to move: %s", ids_to_move)
        dest_butler.transfer_from(
            butler,
            source_refs=datasetRefs_no_exposure,
            transfer="copy",
            skip_missing=True,
            register_dataset_types=True,
            transfer_dimensions=True,
        )
        if namespace.log == "True":
            ids_moved = [
                dt.id
                for dt in dest_registry.queryDatasets(
                    datasetType=datalist_no_exposure, collections=collections_no_exposure
                )
            ]
            logger.info("ingest ids moved: %s", ids_moved)

    if move == "True":
        # prune both datasettypes
        # is there a way to do this at the same time?
        if datalist_exposure:
            butler.pruneDatasets(refs=datasetRefs_exposure, unstore=True, purge=True)
        if datalist_no_exposure:
            butler.pruneDatasets(refs=datasetRefs_no_exposure, unstore=True, purge=True)
