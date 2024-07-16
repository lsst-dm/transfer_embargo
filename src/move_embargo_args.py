import argparse
import logging
import yaml

import astropy.time
import os
import tempfile
from lsst.resources import ResourcePath
from lsst.daf.butler import Butler, Timespan, FileDataset
from lsst.daf.butler.cli.cliLog import CliLog


def parse_args():
    """Parses and returns command-line arguments
    for transferring data between Butler repositories.

    Extended Summary
    ----------------
    The function sets up an argument parser for transferring
    data from an embargo Butler repository to another Butler repository.
    It defines several arguments and their options, including source
    and destination repositories, instrument, embargo time periods,
    configuration options, and more.

    Returns
    -------
        argparse.Namespace: An object containing the parsed command-line
        arguments.

    Arguments
    ---------
    fromrepo : `str`
        Path to the Butler repository from which data is transferred.
        Default is '/repo/embargo'.
    torepo : `str`
        Path to the Butler repository to which data is transferred.
    instrument : `str`
        Instrument name. Default is 'LATISS'.
    pastembargohours : `float`, optional
        Time to search past the embargo period in hours.
        Useful for transferring limited data during initial testing.
    embargohours : `list` [`float`], optional
        Embargo time period in hours.
    use_dataquery_config : `bool`, optional
        If True, uses the configuration from the config file;
        if False, uses provided datasettype and collections lists.
    dataquery_config_file : `str`, optional
        Path to the configuration file. Default is '/etc/config.yaml'.
    datasettype : `list` [`str`], optional
        Dataset type(s).
    collections : `list` [`str`], optional
        Data collections. Default is 'LATISS/raw/all'.
    nowtime : `list` [`str`], optional
        Current time in ISO, TAI timescale. Default is 'now'.
    move : `bool`, optional
        If True, deletes original data after transfer; if False, copies data.
    log : `str`, optional
        Logging level. Default is 'INFO'.
        Other options are 'DEBUG' and 'WARNING'.
    desturiprefix : `str`, optional
        Destination URI prefix for raw data ingestion. Default is 'False'.

    Example
    -------
        args = parse_args()
        from_repo = args.fromrepo
        to_repo = args.torepo
        instrument = args.instrument
    """
    parser = argparse.ArgumentParser(
        description="Transferring data from embargo butler to another butler"
    )
    parser.add_argument(
        "fromrepo",
        type=str,
        nargs="?",
        default="/repo/embargo",
        help="Butler repository path from which data is transferred. Input str. Default = '/repo/embargo'",
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
        nargs="+",
        # type=float,
        required=False,
        # default=80.0,
        help="Embargo time period in hours. Input float or list",
    )
    parser.add_argument(
        "--pastembargohours",
        type=float,
        required=False,
        help="Time to search past the embargo time period in hours. \
              Input float. Defines a window of data that will be transferred. \
              This is helpful for transferring a limited amount of data \
              during the initial testing of the deployment.",
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
        "--dataquery_config_file",
        required=False,
        type=str,
        default="/etc/config.yaml",
        help="Name and path of the config file.\
              Used to input datasettype, collections list pairs.\
              If datasettype and collections args are also provided,\
              these are the default. \
              Default is /etc/config.yaml",
    )
    parser.add_argument(
        "--dataqueries",
        required=False,
        type=str,
        help="A dictionary of dataset type and collection, \
              can be a single entry or multiple items. Str",
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
        help="Copies if False, deletes original if it exists (if True)",
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
        help="Define dest URI if you need to run ingest for raws",
    )
    return parser.parse_args()


if __name__ == "__main__":
    namespace = parse_args()
    # Define embargo and destination butler
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

    # If move is true, then you'll need write
    # permissions from the fromrepo (embargo)
    # For now we will not allow move to be true
    if namespace.move:
        raise ValueError(
            "namespace.move is True. Program terminating because this is too dangerous."
        )

    # determine if we will use the config file or the
    # provided datasettypelist and collections args
    # from the cli
    if namespace.use_dataquery_config:
        # define the config file
        config_file = namespace.dataquery_config_file
        logger.info("using the config file, config_file name/path is %s", config_file)
        # read config file
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        logger.info("config %s", config)
        # Extract datasettype and collections from config
        datasetTypeList = []
        collections = []
        embargohrs_list = []
        for query in config["dataqueries"]:
            datasetTypeList.append(query["datasettype"])
            collections.append(query["collections"])
            if "embargohrs" in query:
                embargohrs_list.append(query["embargohrs"])
    else:
        # parse the JSON string into a dictionary
        logger.info("Using dataqueries: %s", namespace.dataqueries)
        if namespace.dataqueries:
            try:
                dataqueries_dict = yaml.safe_load(namespace.dataqueries)
                print("Parsed dictionary:")
                for key, value in dataqueries_dict.items():
                    print(f"{key}: {value}")
            except yaml.YAMLError as e:
                print(f"Error parsing YAML string: {e}")
        else:
            dataqueries_dict = {}
            logger.info(
                "No data queries provided and no config provided, \
                         we have a problem."
            )

        datasetTypeList = dataqueries_dict["datasettype"]
        collections = dataqueries_dict["collections"]
        # make these iterable if they are not
        if not isinstance(datasetTypeList, list):
            datasetTypeList = [datasetTypeList]
            collections = [collections]

        # datasetTypeList = namespace.datasettype
        # collections = namespace.collections

    logger.info("whats the datasettypelist in here: %s", datasetTypeList)
    logger.info("type of the datasettypelist in here: %s", type(datasetTypeList))

    move = namespace.move
    if namespace.desturiprefix:
        dest_uri_prefix = namespace.desturiprefix
    else:
        # if this is not defined, make a tempdir
        temp_dir = tempfile.TemporaryDirectory()
        dest_uri_prefix = os.path.join(temp_dir.name, "temp_dest_ingest")

    # Dataset to move
    dataId = {"instrument": namespace.instrument}

    # Convert embargohours and nowtime to lists if they are not already
    namespace.embargohours = (
        namespace.embargohours
        if isinstance(namespace.embargohours, list)
        else [namespace.embargohours]
    )
    namespace.nowtime = (
        namespace.nowtime
        if isinstance(namespace.nowtime, list)
        else [namespace.nowtime]
    )

    logger.info("embargo  hrs: %s", namespace.embargohours)
    logger.info("past embargo hrs: %s", namespace.pastembargohours)
    logger.info("nowtime: %s", namespace.nowtime)

    # Calculate embargo periods for each embargo hour
    embargo_hours = [float(hours) for hours in namespace.embargohours]
    embargo_periods = [
        astropy.time.TimeDelta(hours * 3600.0, format="sec") for hours in embargo_hours
    ]

    now_list = []
    for nows in namespace.nowtime:
        if nows != "now":
            now_list.append(astropy.time.Time(nows, scale="tai", format="iso"))
        else:
            now_list.append(astropy.time.Time.now().tai)

    timespans_embargo = []
    timespans_pastembargo = []

    # Log now_list and embargo_periods
    logger.info("now list: %s", now_list)
    logger.info("embargo_periods: %s", embargo_periods)

    for i in range(len(now_list)):
        now = now_list[i]
        embargo_period = embargo_periods[i]
        logger.info("i: %s", i)
        logger.info("now list entry: %s", now)
        logger.info("embargo_periods entry: %s", embargo_period)

        if namespace.pastembargohours:
            past_embargo_period = astropy.time.TimeDelta(
                namespace.pastembargohours * 3600.0, format="sec"
            )
            start_of_embargo = now - embargo_period
            end_of_embargo = now - embargo_period - past_embargo_period

            logger.info("using past embargohours: %s", namespace.pastembargohours)
            logger.info("beginning of span: %s", start_of_embargo)
            logger.info("end of span: %s", end_of_embargo)

            timespan_embargo = Timespan(end_of_embargo, start_of_embargo)
            timespans_embargo.append(timespan_embargo)
            assert (
                start_of_embargo > end_of_embargo
            ), "end of embargo happens before start of embargo, this is grabbing not yet released data"
        else:
            timespan_embargo = Timespan(now - embargo_period, None)
            timespans_embargo.append(timespan_embargo)

    # Log the list of timespans
    logger.info("list of timespans: %s", timespans_embargo)

    logger.info("from path: %s", namespace.fromrepo)
    logger.info("to path: %s", namespace.torepo)

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
        logger.info("dtype: %s", dtype)
        logger.info(
            "registry.queryDatasetTypes(dtype): %s", registry.queryDatasetTypes(dtype)
        )
        logger.info("collections: %s", collections)
        logger.info(
            "registry.queryDatasetTypes(dtype): %s", registry.queryDatasetTypes(dtype)
        )
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
                        logger.info(
                            "dest_uri_prefix: %s",
                            dest_uri_prefix)
                        logger.info(
                            "new_dest_uri does not yet exist: %s",
                            new_dest_uri)
                        logger.info(
                            "source path URI: %s",
                            source_path_uri)
                        new_dest_uri.transfer_from(source_path_uri, transfer="copy")
                        logger.info(
                            "new_dest_uri does not exist (%s), creating new dest URI",
                            new_dest_uri
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
        logger.info("datalist_visit exists: %s", datalist_visit)
        logger.info("collections: %s", collections_visit)
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
        logger.info("ids in to butler: %s", ids_moved)

    if move == "True":
        # concatenate both dataset types
        combined_datalist = datalist_exposure + datalist_visit + datalist_no_exposure
        # prune the combined list only if it is defined
        if combined_datalist:
            combined_dataset_refs = (
                datasetRefs_exposure + datasetRefs_visit + datasetRefs_no_exposure
            )
            butler.pruneDatasets(refs=combined_dataset_refs, unstore=True, purge=True)
