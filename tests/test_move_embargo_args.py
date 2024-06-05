import os
import shutil
import subprocess
import tempfile
import unittest
import yaml

import lsst.utils as utils
import pytest
from lsst.daf.butler import Butler


def is_it_there(
    ids_should_remain_after_move,
    ids_should_be_moved,
    temp_from,
    temp_to,
    move=None,
    log: str = "INFO",
    embargo_hours: list | str = "80.0",
    past_embargo_hours=None,
    now_time_embargo: list | str = "now",
    datasettype: list | str = "raw",
    collections: list | str = "LATISS/raw/all",
    desturiprefix: str = "tests/data/",
    use_dataquery_config=None,
    dataquery_config_file: str = "./config.yaml",
):
    """
    # Convert single values to lists if needed
    if not isinstance(embargo_hours, list):
        embargo_hours = [embargo_hours]
    if not isinstance(now_time_embargo, list):
        now_time_embargo = [now_time_embargo]
    """
    # Run the package
    print("this is the move arg", str(move) if move is not None else "")
    # start by adding common args for both the config
    # and the cli options
    subprocess_args = [
        "python",
        "../src/move_embargo_args.py",
        temp_from,
        temp_to,
        "LATISS",
        "--log",
        log,
        "--desturiprefix",
        desturiprefix,
    ]
    if use_dataquery_config:
        # define the config path
        config_file = dataquery_config_file
        # read config file
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        # extract datasettype and collections from config
        print("config in its entirety", config)
        datasettype = []
        collections = []
        for query in config["dataqueries"]:
            datasettype.append(query["datasettype"])
            collections.append(query["collections"])
            print(f"Dataset Type: {datasettype}, Collections: {collections}")
    else:
        # need to check if datasettype is a single str,
        # make it iterable if so
        iterable_embargo_hours = utils.iteration.ensure_iterable(embargo_hours)
        iterable_nowtime = utils.iteration.ensure_iterable(now_time_embargo)
        iterable_datasettype = utils.iteration.ensure_iterable(datasettype)
        iterable_collections = utils.iteration.ensure_iterable(collections)
        # and extend the args to include the cli args
        subprocess_args.extend(
            [
                "--embargohours",
                *iterable_embargo_hours,
                "--nowtime",
                *iterable_nowtime,
                "--datasettype",
                *iterable_datasettype,
                "--collections",
                *iterable_collections,
            ]
        )
    # add --move argument only if move is not None
    if move is not None:
        subprocess_args.extend(["--move"])
    # so we do not want to test in cases where move is True
    # we are currently only testing where move is False
    # so the following assert should cause the test to fail
    # if move is True
    assert move is None, f"move is {move}"
    # do the same with the past embargo hours arg
    if past_embargo_hours is not None:
        subprocess_args.extend(
            ["--pastembargohours", past_embargo_hours]
        )  # , str(move)])

    if use_dataquery_config is not None:
        iterable_embargo_hours = utils.iteration.ensure_iterable(embargo_hours)
        iterable_nowtime = utils.iteration.ensure_iterable(now_time_embargo)
        subprocess_args.extend(
            [
                "--embargohours",
                *iterable_embargo_hours,
                "--nowtime",
                *iterable_nowtime,
                "--use_dataquery_config",
                "--dataquery_config_file",
                str(dataquery_config_file),
            ]
        )  # , str(move)])
    print("loaded the config")
    # now run the subprocess
    subprocess.run(subprocess_args, check=True)
    print("made it through the program")

    # first test stuff in the temp_to butler
    butler_to = Butler(temp_to)
    registry_to = butler_to.registry
    counter = 0
    ids_in_temp_to = []
    for dtype in datasettype:
        # if there is nothing in registry_to, should enter the following
        # if not statement
        # this is necessary in order to not trigger an indexerror
        # when trying to do registry_to.queryDatasetTypes(dtype)[0]
        if not registry_to.queryDatasetTypes(dtype):
            # then the list is empty
            print("empty temp to")
            counter += 1
            continue
        if any(
            dim in ["visit"]
            for dim in registry_to.queryDatasetTypes(dtype)[0].dimensions.names
        ):
            ids_visit = [
                dt.dataId.mapping["visit"]
                for dt in registry_to.queryDatasets(datasetType=dtype, collections=...)
            ]
            for id in ids_visit:
                ids_in_temp_to.append(id)

        elif any(
            dim in ["exposure"]
            for dim in registry_to.queryDatasetTypes(dtype)[0].dimensions.names
        ):
            ids_exposure = [
                dt.dataId.mapping["exposure"]
                for dt in registry_to.queryDatasets(datasetType=dtype, collections=...)
            ]
            for id in ids_exposure:
                ids_in_temp_to.append(id)
        else:
            datasetRefs = registry_to.queryDatasets(
                datasetType=dtype, collections=collections
            )
            ids_no_exposure = [dt.id for dt in datasetRefs]
            for id in ids_no_exposure:
                ids_in_temp_to.append(id)
        counter += 1
    assert (
        counter != 0
    ), f"Never went through the dtype for loop shame on you, \
       counter = {counter}"

    # verifying the contents of the temp_to butler
    # check that what we expect to move (ids_should_be_moved)
    # are in the temp_to repo (ids_in_temp_to)
    print("stuff that should move", ids_should_be_moved)
    sorted_moved = sorted(ids_should_be_moved)
    sorted_temp_to = sorted(ids_in_temp_to)
    message = f"{sorted_moved} should be in {temp_to} repo but is not, instead what is there:{sorted_temp_to}"
    print(message[:1000])  # print the first part
    print(message[1000:])  # print the second part

    assert sorted_moved == sorted_temp_to, message

    # now check the temp_from butler and see what remains
    butler_from = Butler(temp_from)
    registry_from = butler_from.registry
    ids_in_temp_from_exposure = []
    ids_in_temp_from_visit = []
    ids_in_temp_from_else = []
    # explore the datatypes in registry from
    # build a list of all ids in registry from
    for DatasetType in registry_from.queryDatasetTypes(...):
        if any(dim in ["exposure"] for dim in DatasetType.dimensions.names):
            ids_in_temp_from_exposure = [
                dt.dataId.mapping["exposure"]
                for dt in registry_from.queryDatasets(
                    datasetType=DatasetType.name, collections=...
                )
            ]
        elif any(dim in ["visit"] for dim in DatasetType.dimensions.names):
            ids_in_temp_from_visit = [
                dt.dataId.mapping["visit"]
                for dt in registry_from.queryDatasets(
                    datasetType=DatasetType.name, collections=...
                )
            ]
        else:
            ids_in_temp_from_else = [
                dt.id
                for dt in registry_from.queryDatasets(
                    datasetType=DatasetType.name, collections=...
                )
            ]
    # now concatenate all of these:
    ids_in_temp_from = (
        ids_in_temp_from_exposure + ids_in_temp_from_visit + ids_in_temp_from_else
    )

    # verify the contents of the from butler
    # the list of ids in ids_should_be_in_temp_from
    # must be included in the list of ids actually
    # in temp from
    missing_ids = [
        id_should_be
        for id_should_be in ids_should_remain_after_move
        if id_should_be not in ids_in_temp_from
    ]
    assert (
        not missing_ids
    ), f"move is {move} and the following IDs are missing in {temp_from} repo: {missing_ids}, \
        instead this is what is in it: {ids_in_temp_from}"


class AtLeastOneAssertionFailedError(Exception):
    pass


class TestMoveEmbargoArgs(unittest.TestCase):
    def setUp(self):
        """
        Performs the setup necessary to run
        all tests
        """
        temp_dir = tempfile.TemporaryDirectory()
        temp_from_path = os.path.join(temp_dir.name, "temp_test_from")
        temp_to_path = os.path.join(temp_dir.name, "temp_test_to")
        temp_dest_ingest = os.path.join(temp_dir.name, "temp_dest_ingest")
        shutil.copytree("./data/test_from", temp_from_path)
        os.system("chmod u+x create_testto_butler.sh")
        subprocess.call(
            [
                "./create_testto_butler.sh",
                temp_to_path,
            ]
        )
        self.temp_dir = temp_dir
        self.temp_from_path = temp_from_path
        self.temp_to_path = temp_to_path
        self.temp_dest_ingest = temp_dest_ingest
        # The above is if we are running 'move',
        # If copy, it should be both of these
        # added together
        self.log = "INFO"

    def tearDown(self):
        """
        Removes all test files created by tests
        """
        shutil.rmtree(self.temp_dir.name, ignore_errors=True)

    # first a a big group of calexp tests
    def test_calexp_should_copy_yaml_pasttime_18_half_hr(self):
        """
        Test that move_embargo_args runs for the calexp datatype
        read from the config.yaml file
        """
        now_time_embargo = "2022-11-13 03:35:12.836981"
        # '2022-11-09 01:03:22.888003'
        # "2020-01-17 16:55:11.322700"
        embargo_hours = str(80.0)  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [2022110800238]
        # IDs that should stay in the temp_from:
        ids_remain = [2022110800235, 2022110800230, 2022110800238]
        is_it_there(
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            past_embargo_hours=str(18.5),
            now_time_embargo=now_time_embargo,
            desturiprefix=self.temp_dest_ingest,
            use_dataquery_config=True,
            dataquery_config_file="./yamls/config_calexp.yaml",
        )

    def test_calexp_should_copy_yaml(self):
        """
        Test that move_embargo_args runs for the calexp datatype
        read from the config.yaml file
        """
        now_time_embargo = "2022-11-13 03:35:12.836981"
        # '2022-11-09 01:03:22.888003'
        # "2020-01-17 16:55:11.322700"
        embargo_hours = str(80.0)  # hours
        # IDs that should be copied to temp_to:
        ids_copied = [2022110800235, 2022110800230, 2022110800238]
        # IDs that should stay in the temp_from:
        ids_remain = [2022110800235, 2022110800230, 2022110800238]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            desturiprefix=self.temp_dest_ingest,
            use_dataquery_config=True,
            dataquery_config_file="./yamls/config_calexp.yaml",
        )

    def test_calexp_yaml_pasttime_1_hr(self):
        """
        Test that move_embargo_args runs for the calexp datatype
        read from the config.yaml file
        """
        now_time_embargo = "2022-11-13 03:35:12.836981"
        # '2022-11-09 01:03:22.888003'
        # "2020-01-17 16:55:11.322700"
        embargo_hours = str(80.0)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = []
        # IDs that should stay in the temp_from:
        ids_remain = [2022110800235, 2022110800230, 2022110800238]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            past_embargo_hours=str(1.0),
            now_time_embargo=now_time_embargo,
            desturiprefix=self.temp_dest_ingest,
            use_dataquery_config=True,
            dataquery_config_file="./yamls/config_calexp.yaml",
        )

    def test_calexp_no_copy(self):
        """
        Test that move_embargo_args does not move
        the calexp data that is too close to embargo
        """
        now_time_embargo = "2022-11-11 03:35:12.836981"
        # "2020-01-17 16:55:11.322700"
        embargo_hours = str(80.0)  # hours
        ids_copied = []
        # IDs that should stay in the temp_from:
        ids_remain = [2022110800235, 2022110800230, 2022110800238]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["calexp"],
            collections=[
                "LATISS/runs/AUXTEL_DRP_IMAGING_2022-11A/w_2022_46/PREOPS-1616"
            ],
            desturiprefix=self.temp_dest_ingest,
            # desturiprefix="tests/data/",
        )

    '''
    # commenting out this one test that incorporates multiple
    # embargohrs arguments from the config yaml
    def test_raw_and_calexp_should_move_yaml_embargo_hrs_in_yaml(self):
        """
        Test that move_embargo_args runs for the calexp datatype
        and for the raw datatype at the same time
        """
        # first raw, then calexp
        now_time_embargo = ["2020-01-17 16:55:11.322700",
                            "2022-11-13 03:35:12.836981"]
        # IDs that should be moved to temp_to:
        ids_moved = [
            # 2020011700004,
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2022110800235, 2022110800230, 2022110800238
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
            2022110800235, 2022110800230, 2022110800238
        ]
        is_it_there(
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            now_time_embargo=now_time_embargo,
            use_dataquery_config=True,
            dataquery_config_file="./yamls/config_all_embargohrs.yaml",
            desturiprefix=self.temp_dest_ingest,
        )
    '''

    def test_calexp_should_copy(self):
        """
        Test that move_embargo_args runs for the calexp datatype
        """
        now_time_embargo = "2022-11-13 03:35:12.836981"
        # '2022-11-09 01:03:22.888003'
        # "2020-01-17 16:55:11.322700"
        embargo_hours = str(80.0)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [2022110800235, 2022110800230, 2022110800238]
        # IDs that should stay in the temp_from:
        ids_remain = [2022110800235, 2022110800230, 2022110800238]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["calexp"],
            collections=[
                "LATISS/runs/AUXTEL_DRP_IMAGING_2022-11A/w_2022_46/PREOPS-1616"
            ],
            desturiprefix=self.temp_dest_ingest,
            # desturiprefix="tests/data/",
        )

    def test_raw_and_calexp_should_copy_yaml(self):
        """
        Test that move_embargo_args runs for the calexp datatype
        and for the raw datatype at the same time
        """
        # first raw, then calexp
        now_time_embargo = ["2020-01-17 16:55:11.322700",
                            "2022-11-13 03:35:12.836981"]
        embargo_hours = [str(0.1),
                         str(80.0)]  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            # 2020011700004,
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2022110800235,
            2022110800230,
            2022110800238,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
            2022110800235,
            2022110800230,
            2022110800238,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            use_dataquery_config=True,
            dataquery_config_file="./yamls/config_all.yaml",
            desturiprefix=self.temp_dest_ingest,
        )

    def test_raw_should_copy_yaml(self):
        """
        Verify that exposures after now are not being moved
        when the nowtime is right in the middle of the exposures
        Test this for reading from the yaml
        """
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = str(0.1)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            # 2020011700004,
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            desturiprefix=self.temp_dest_ingest,
            use_dataquery_config=True,
            dataquery_config_file="./yamls/config_raw.yaml",
        )

    # potentially we won't need to test this in the future
    # @KT - we were not planning on running multiple args from
    # cli
    def test_raw_and_calexp_should_copy(self):
        """
        Test that move_embargo_args runs for the calexp datatype
        and for the raw datatype at the same time
        """
        # first raw, then calexp
        now_time_embargo = ["2020-01-17 16:55:11.322700", "2022-11-13 03:35:12.836981"]
        embargo_hours = [str(0.1), str(80.0)]  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            # 2020011700004,
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2022110800235,
            2022110800230,
            2022110800238,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
            2022110800235,
            2022110800230,
            2022110800238,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw", "calexp"],
            collections=[
                "LATISS/raw/all",
                "LATISS/runs/AUXTEL_DRP_IMAGING_2022-11A/w_2022_46/PREOPS-1616",
            ],
            desturiprefix=self.temp_dest_ingest,
        )

    @pytest.mark.xfail(strict=True)
    def test_should_fail_if_move_is_true(self):
        """
        Move being true is scary because it deletes everything
        in the source repo. Let's make sure that move_embargo_args
        has a mechnism in place to fail if you set move to be true
        """
        move = "anything"
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = str(0.1)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_after_now_01(self):
        """
        Verify that exposures after now are not being moved
        when the nowtime is right in the middle of the exposures
        """
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = str(0.1)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            # 2020011700004,
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_nothing_copies(self):
        """
        Nothing should move when the embargo hours falls right on
        the oldest exposure
        """
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = str(5596964.255774 / 3600.0)
        # IDs that should be moved to temp_to:
        ids_copied = []
        # IDs that should stay in the temp_from:
        ids_remain = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    # test the other datatypes:
    # first goodseeingdeepcoadd
    def test_raw_datatypes(self):
        """
        Test that move_embargo_args runs for a list
        of input datatypes
        """
        # now_time_embargo = "now"
        # embargo_hours =  80.0 # hours
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = str(0.1)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    @pytest.mark.xfail(strict=True)
    def test_raw_datatypes_should_fail(self):
        """
        Test that move_embargo_args runs for a list
        of input datatypes
        """
        # now_time_embargo = "now"
        # embargo_hours =  80.0 # hours
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = str(0.1)  # hours
        # IDs that should be moved to temp_to:
        # lol 2019111300059 should be in the ids_moved
        # list but I'm removing it to make sure the assertions
        # fail
        ids_copied = [
            2019111300061,
            2020011700002,
            2020011700003,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
        ]

        # try:
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_after_now_05(self):
        """
        Verify that exposures after now are not being moved
        when the nowtime is right in the middle of the exposures
        for a slightly longer embargo period (0.5 hours)
        """
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = str(0.5)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            2019111300059,
            2019111300061,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700002,
            2020011700003,
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_copy(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        now_time_embargo = "2020-03-01 23:59:59.999999"
        embargo_hours = str(3827088.677299 / 3600)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2020011700004,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_copy_midnight(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        now_time_embargo = "2020-03-02 00:00:00.000000"
        embargo_hours = str(3827088.6773 / 3600)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2020011700004,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_midnight_precision(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        now_time_embargo = "2020-03-02 00:00:00.000000"
        embargo_hours = str(3827088.677301 / 3600)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_copy_midnight_precision(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        now_time_embargo = "2020-03-02 00:00:00.000000"
        embargo_hours = str(3827088.677301 / 3600)  # hours
        # IDs that should be moved to temp_to:
        ids_copied = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
        ]
        # IDs that should stay in the temp_from:
        ids_remain = [
            2020011700004,
            2020011700005,
            2020011700006,
        ]
        is_it_there(
            ids_remain,
            ids_copied,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            embargo_hours=embargo_hours,
            now_time_embargo=now_time_embargo,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )


if __name__ == "__main__":
    unittest.main()
