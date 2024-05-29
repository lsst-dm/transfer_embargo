import argparse
import logging
import yaml

import astropy.time
from lsst.resources import ResourcePath
from lsst.daf.butler import Butler, Timespan, FileDataset
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
        "--pastembargohours",
        type=str,
        required=False,
        help="Time to search past the embargo time period in hours. \
              Input float or list. This is helpful for not transferring everything \
              during the initial testing of the deployment.",
    )
    parser.add_argument(
        "--embargohours",
        nargs="+",
        # type=float,
        required=False,
        # default=80.0,
        help="Embargo time period in hours. Input float or list",
    )
    parser.add_argument(
        "--use_dataquery_config",
        required=False,
        action="store_true",
        help="Ignores config.yaml and uses datasettype and collections lists if False. \
              If this keyword is used (if True), reads from the config file.\
              The path and name of the config file are given below",
    )
    parser.add_argument(
        "--dataquery_config_file_name",
        type=str,
        required=False,
        default="config.yaml",
        help="Config file that contains lists of datasettype and collections",
    )
    parser.add_argument(
        "--dataquery_config_file_path",
        required=False,
        type=str,
        default="/etc/",
        help="Name and path of the config file.\
              Used to input datasettype, collections list pairs.\
              If datasettype and collections args are also provided,\
              these are the default.",
    )
    parser.add_argument(
        "--datasettype",
        required=False,
        nargs="+",
        help="Dataset type. Input list or str",
    )
    parser.add_argument(
        "--collections",
        nargs="+",
        required=False,
        default="LATISS/raw/all",
        help="Data Collections. Input list or str",
    )
    parser.add_argument(
        "--nowtime",
        nargs="+",
        default="now",
        help="Now time in (ISO, TAI timescale). If left blank it will \
              use astropy.time.Time.now. List or str",
    )
    parser.add_argument(
        "--move",
        required=False,
        action="store_true",
        help="Copies if False, deletes original if it exists True",
    )
    parser.add_argument(
        "--log",
        type=str,
        required=False,
        default="INFO",
        help="Default is INFO level, other options are DEBUG or WARNING",
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

    # Initialize the logger and set the level
    CliLog.initLog(longlog=True)
    CliLog.setLogLevels(logLevels=[(None, namespace.log)])
    # CliLogNew.initLog(log=namespace.log)
    logger = logging.getLogger("lsst.transfer.embargo")
    logger.info("log level %s", namespace.log)
    logger.info("namespace: %s", namespace)
    if namespace.move:
        raise ValueError(
            "namespace.move is True. Program terminating because this is too dangerous."
        )

    # determine if we will use the config file or the
    # provided datasettypelist and collections args
    if namespace.use_dataquery_config:
        logger.info(
            "using the config file, use_dataquery_config is %s",
            namespace.use_dataquery_config,
        )
        # define the config path
        config_file = (
            namespace.dataquery_config_file_path + namespace.dataquery_config_file_name
        )
        logger.info("config_file name/path is %s", config_file)
        # Read config file
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        print("config", config)
        logger.info("config %s", config)
        # Extract datasettype and collections from config
        datasetTypeList = []
        collections = []
        embargohrs_list = []
        # dataqueries = []
        for query in config["dataqueries"]:
            # dataqueries.append(DataQuery(query))
            datasetTypeList.append(query["datasettype"])
            collections.append(query["collections"])
            if "embargohrs" in query:
                # The 'embargohrs' key exists in the dictionary 'query'
                embargohrs_list.append(query["embargohrs"])
    else:
        datasetTypeList = namespace.datasettype
        collections = namespace.collections

    logger.info("whats the datasettypelist in here: %s", datasetTypeList)

    move = namespace.move
    dest_uri_prefix = namespace.desturiprefix
    # Dataset to move
    dataId = {"instrument": namespace.instrument}
    # Define embargo period and nowtime
    logger.info("embargo  hrs: %s", namespace.embargohours)
    logger.info("past embargo hrs: %s", namespace.pastembargohours)
    logger.info("nowtime: %s", namespace.nowtime)

    # option for embargohours and nowtime to be individual items
    if len(namespace.embargohours) == 1 and len(namespace.nowtime) == 1:
        embargo_period = astropy.time.TimeDelta(
            float(namespace.embargohours[0]) * 3600.0, format="sec"
        )
        if namespace.nowtime != "now":
            now = astropy.time.Time(namespace.nowtime[0], scale="tai", format="iso")
        else:
            now = astropy.time.Time.now().tai
        # the timespan object defines a "forbidden" region of time
        # starting at the nowtime minus the embargo period
        # and terminating in anything in the future
        # this forbidden timespan will be de-select
        # for moving any exposure that overlaps with it
        # documentation here:
        # https://community.lsst.org/t/constructing-a-where-for-query-dimension-records/6478

        # both for the case of pastembargohours being defined
        # and for when its not defined you'll need the timespan that is
        # embargoed (no transfer allowed here)
        # the format of this is:
        # (start of embargo, infinitely far into the future)
        logger.info("now: %s", now)
        logger.info("embargo_period: %s", embargo_period)
        timespan_embargo = Timespan(now - embargo_period, None)
        logger.info("timespan: %s", timespan_embargo)
        if namespace.pastembargohours:
            logger.info("using past embargohours", namespace.pastembargohours)
            # if this argument is specified, we're placing a limit on the
            # amount of data before that to be transferred
            past_embargo_period = astropy.time.TimeDelta(
                float(namespace.pastembargohours) * 3600.0, format="sec"
            )
            timespan_pastembargo = Timespan(
                now - embargo_period - past_embargo_period, now - embargo_period
            )
            assert (now - embargo_period) > (now - embargo_period - past_embargo_period), \
                "end of embargo happens before start of embargo, this is grabbing not yet released data"
    elif len(namespace.embargohours) > 1 and len(namespace.nowtime) > 1:
        embargo_hours = [float(hours) for hours in namespace.embargohours]
        # Calculate embargo_period for each embargo hour
        embargo_periods = [
            astropy.time.TimeDelta(hours * 3600.0, format="sec")
            for hours in embargo_hours
        ]
        now_list = []
        for nows in namespace.nowtime:
            logger.info("nows: %s", nows)
            if nows != "now":
                now_list.append(astropy.time.Time(nows, scale="tai", format="iso"))
            else:
                now_list.append(astropy.time.Time.now().tai)
        timespans_embargo = []
        timespans_pastembargo = []
        # this is assuming both nows and embargo periods are specified, lists
        # are the same length, required to specify both
        logger.info("now list: %s", now_list)
        logger.info("length of this: %s", len(now_list))

        for i in range(len(now_list)):
            logger.info("i: %s", i)
            logger.info("now list entry: %s", now_list[i])
            logger.info("embargo_periods entry: %s", embargo_periods[i])
            timespans_embargo.append(Timespan(now_list[i] - embargo_periods[i], None))
            if namespace.pastembargohours:
                logger.info("using past embargohours")
                # if this argument is specified, we're placing a limit on the
                # amount of data before that to be transferred
                past_embargo_periods = [
                    astropy.time.TimeDelta(
                        float(namespace.pastembargohours) * 3600.0, format="sec"
                    )
                    for hours in embargo_hours
                ]
                start_of_embargo = now_list[i] - embargo_periods[i]
                end_of_embargo = (
                    now_list[i] - embargo_periods[i] - past_embargo_periods[i]
                )
                logger.info("beginning of span %s:", start_of_embargo)
                logger.info("end of span %s:", end_of_embargo)
                timespans_pastembargo.append(Timespan(end_of_embargo, start_of_embargo))
        logger.info("list of timespans: %s", timespans_embargo)

    else:
        # this means that one is a list but one is not
        # and that is a problem because we're not prepared for this
        logger.info("neither is a list apparently")
        logger.info("namespace.embargohours %s:", namespace.embargohours)
        logger.info("namespace.nowtime %s:", namespace.nowtime)
        # Stop the program and print out log messages
        assert False, "Namespace embargohours and nowtime are not handled correctly"

    logger.info("from path: %s", namespace.fromrepo)
    logger.info("to path: %s", namespace.torepo)

    if len(namespace.embargohours) == 1 and len(namespace.nowtime) == 1:
        # The Dimensions query
        # If (now - embargo period, now) does not overlap
        # with observation time interval: move
        # Else: don't move
        # Save data Ids of these observations into a list
        datalist_exposure = []
        collections_exposure = []

        dataquery_exposure = []

        datalist_visit = []
        collections_visit = []

        # configs have no dims
        # also data by tract, patch, not connected to original images
        # ie coadds
        datalist_no_exposure = []
        collections_no_exposure = []

        for i, dtype in enumerate(datasetTypeList):
            if any(
                dim in ["visit"]
                for dim in registry.queryDatasetTypes(dtype)[0].dimensions.names
            ):
                datalist_visit.append(dtype)
                collections_visit.append(collections[i])
            elif any(
                dim in ["exposure"]
                for dim in registry.queryDatasetTypes(dtype)[0].dimensions.names
            ):
                datalist_exposure.append(dtype)
                collections_exposure.append(collections[i])
            else:
                # these should be the raw datasettype
                datalist_no_exposure.append(dtype)
                collections_no_exposure.append(collections[i])
    elif len(namespace.embargohours) > 1 and len(namespace.nowtime) > 1:
        datalist_exposure = []
        collections_exposure = []
        timespan_embargo_exposure = []

        datalist_visit = []
        collections_visit = []
        timespan_embargo_visit = []

        datalist_no_exposure = []
        collections_no_exposure = []
        timespan_embargo_no_exposure = []

        for i, dtype in enumerate(datasetTypeList):
            if any(
                dim in ["visit"]
                for dim in registry.queryDatasetTypes(dtype)[0].dimensions.names
            ):
                datalist_visit.append(dtype)
                collections_visit.append(collections[i])
                timespan_embargo_visit.append(timespans_embargo[i])
            elif any(
                dim in ["exposure"]
                for dim in registry.queryDatasetTypes(dtype)[0].dimensions.names
            ):
                datalist_exposure.append(dtype)
                collections_exposure.append(collections[i])
                timespan_embargo_exposure.append(timespans_embargo[i])
            else:
                # these should be the raw datasettype
                datalist_no_exposure.append(dtype)
                collections_no_exposure.append(collections[i])
                timespan_embargo_no_exposure.append(timespans_embargo[i])
    # sort out which dtype goes into which list
    logger.info("datalist_exposure to move: %s", datalist_exposure)
    logger.info("datalist_visit to move: %s", datalist_visit)
    logger.info("datalist_no_exposure to move: %s", datalist_no_exposure)
    # because some dtypes don't have an exposure dimension
    # we will need a different option to move those
    # ie deepcoadds
    # but first we will move all dtypes that have an
    # exposure or visit dimension (ie calexp and raw)

    if datalist_exposure:  # if there is anything in the list
        # first, run all of the exposure types through
        if len(namespace.embargohours) == 1 and len(namespace.nowtime) == 1:
            if namespace.pastembargohours:
                outside_embargo = [
                    dt.id
                    for dt in registry.queryDimensionRecords(
                        "exposure",
                        dataId=dataId,
                        datasets=datalist_exposure,
                        collections=collections_exposure,
                        where="exposure.timespan OVERLAPS\
                                                                timespan_embargo",
                        bind={"timespan_embargo": timespan_embargo},
                    )
                ]
            else:
                # where and bind can be separate variables, defined in the if
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

        elif len(namespace.embargohours) > 1 and len(namespace.nowtime) > 1:
            logger.info("timespan_embargo_exposure: %s", timespan_embargo_exposure)
            if namespace.pastembargohours:
                outside_embargo = [
                    dt.id
                    for dt in registry.queryDimensionRecords(
                        "exposure",
                        dataId=dataId,
                        datasets=datalist_exposure,
                        collections=collections_exposure,
                        where="exposure.timespan OVERLAPS\
                                                                    timespan_embargo",
                        bind={"timespan_embargo": timespan_embargo_exposure[0]},
                    )
                ]
            else:
                outside_embargo = [
                    dt.id
                    for dt in registry.queryDimensionRecords(
                        "exposure",
                        dataId=dataId,
                        datasets=datalist_exposure,
                        collections=collections_exposure,
                        where="NOT exposure.timespan OVERLAPS\
                                                                    timespan_embargo",
                        bind={"timespan_embargo": timespan_embargo_exposure[0]},
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
                dest_uri = ResourcePath(dest_uri_prefix)
                source_uri = butler.get_many_uris(datasetRefs_exposure)
                filedataset_list = []
                for key, value in source_uri.items():
                    source_path_uri = value[0]
                    source_path = source_path_uri.relative_to(value[0].root_uri())
                    new_dest_uri = dest_uri.join(source_path)
                    if new_dest_uri.exists():
                        logger.info("new_dest_uri already exists")
                    else:
                        new_dest_uri.transfer_from(source_path_uri, transfer="copy")
                        logger.info(
                            "new_dest_uri does not exist, creating new dest URI"
                        )
                    filedataset_list.append(FileDataset(new_dest_uri, key))

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
        ids_moved = []
        for dt in dest_registry.queryDatasets(
            datasetType=datalist_exposure, collections=collections_exposure
        ):
            try:
                ids_moved.append(dt.dataId.mapping["exposure"])
            except KeyError:
                continue
        """
        ids_moved = [
            dt.dataId.mapping["exposure"]
            for dt in dest_registry.queryDatasets(
                datasetType=datalist_exposure, collections=collections_exposure
            )
        ]
        """
        logger.info("exposure ids moved: %s", ids_moved)
    if datalist_visit:  # if there is anything in the list
        # first, run all of the exposure types through
        logger.info("datalist_visit exists")
        logger.info("collections: %s", collections_visit)
        if len(namespace.embargohours) == 1 and len(namespace.nowtime) == 1:
            if namespace.pastembargohours:
                where_statement = "NOT visit.timespan OVERLAPS timespan_embargo \
                               AND visit.timespan OVERLAPS timespan_pastembargo"
                bind_statement = {
                    "timespan_embargo": timespan_embargo,
                    "timespan_pastembargo": timespan_pastembargo,
                }
            else:
                where_statement = "NOT visit.timespan OVERLAPS\
                                                                timespan_embargo"
                bind_statement = {"timespan_embargo": timespan_embargo}
            logger.info("where statement: %s", where_statement)
            logger.info("bind statement: %s", bind_statement)

            outside_embargo = [
                dt.id
                for dt in registry.queryDimensionRecords(
                    "visit",
                    dataId=dataId,
                    datasets=datalist_visit,
                    collections=...,  # collections_visit,
                    where=where_statement,
                    bind=bind_statement,
                )
            ]
        elif len(namespace.embargohours) > 1 and len(namespace.nowtime) > 1:
            logger.info(
                "these are lists apparently",
                namespace.embargohours,
                namespace.nowtime,
                len(namespace.embargohours),
            )
            if namespace.pastembargohours:
                outside_embargo = [
                    dt.id
                    for dt in registry.queryDimensionRecords(
                        "visit",
                        dataId=dataId,
                        datasets=datalist_visit,
                        collections=...,  # collections_visit,
                        where="visit.timespan OVERLAPS\
                                                                timespan_embargo",  #
                        bind={"timespan_embargo": timespan_embargo_visit[0]},
                    )
                ]
            else:
                outside_embargo = [
                    dt.id
                    for dt in registry.queryDimensionRecords(
                        "visit",
                        dataId=dataId,
                        datasets=datalist_visit,
                        collections=...,  # collections_visit,
                        where="NOT visit.timespan OVERLAPS\
                                                                timespan_embargo",  #
                        bind={"timespan_embargo": timespan_embargo_visit[0]},
                    )
                ]
        logger.info("visit outside embargo: %s", outside_embargo)
        # Query the DataIds after embargo period
        datasetRefs_visit = registry.queryDatasets(
            datalist_visit,
            dataId=dataId,
            collections=...,  # collections_visit,
            where="visit.id IN (visit_ids)",
            bind={"visit_ids": outside_embargo},
        ).expanded()

        ids_to_move = [dt.dataId.mapping["visit"] for dt in datasetRefs_visit]
        logger.info("visit ids to move: %s", ids_to_move)
        # raw dtype requires special handling for the transfer,
        # so separate by dtype:
        for dtype in datalist_visit:
            dest_butler.transfer_from(
                butler,
                source_refs=datasetRefs_visit,
                transfer="copy",
                skip_missing=True,
                register_dataset_types=True,
                transfer_dimensions=True,
            )
        # its breaking here because not everything is a visit in the registry
        ids_moved = []
        for dt in dest_registry.queryDatasets(datasetType=..., collections=...):
            try:
                ids_moved.append(dt.dataId.mapping["visit"])
            except KeyError:
                continue
        """
        ids_moved = [
                dt.dataId.mapping["visit"]
                for dt in dest_registry.queryDatasets(
                datasetType=..., collections=...)
            ]
        """
        logger.info("datalist_visit: %s", datalist_visit)
        logger.info("collections_visit: %s", collections_visit)
        logger.info("visit ids moved: %s", ids_moved)

    if datalist_no_exposure:
        # this is for datatypes that don't have an exposure
        # or visit dimension
        # ie deepcoadds need to be queried using an ingest
        # date keyword
        if not isinstance(namespace.embargohours, list) and not isinstance(
            namespace.nowtime, list
        ):
            datasetRefs_no_exposure = registry.queryDatasets(
                datasetType=datalist_no_exposure,
                collections=collections_no_exposure,
                where="ingest_date <= timespan_embargo_begin",
                bind={"timespan_embargo_begin": timespan_embargo.begin},
            )
        elif isinstance(namespace.embargohours, list) and isinstance(
            namespace.nowtime, list
        ):
            datasetRefs_no_exposure = registry.queryDatasets(
                datasetType=datalist_no_exposure,
                collections=collections_no_exposure,
                where="ingest_date <= timespan_embargo_begin",
                bind={"timespan_embargo_begin": timespan_embargo_no_exposure.begin[0]},
            )
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
        ids_moved = [
            dt.id
            for dt in dest_registry.queryDatasets(
                datasetType=datalist_no_exposure,
                collections=collections_no_exposure,
            )
        ]
        logger.info("ingest ids moved: %s", ids_moved)

    if move == "True":
        # concatenate both dataset types
        combined_datalist = datalist_exposure + datalist_visit + datalist_no_exposure
        # prune the combined list only if it is defined
        if combined_datalist:
            combined_dataset_refs = (
                datasetRefs_exposure + datasetRefs_visit + datasetRefs_no_exposure
            )
            butler.pruneDatasets(refs=combined_dataset_refs, unstore=True, purge=True)
