import subprocess
import unittest
import shutil
import os
import tempfile

import lsst.utils as utils
from typing import Union

from lsst.daf.butler import Butler


def is_it_there(
    embargo_hours: float,
    now_time_embargo: str,
    ids_should_remain_after_move,
    ids_should_be_moved,
    temp_from,
    temp_to,
    move,
    log,
    datasettype: Union[list, str] = "raw",
    collections: Union[list, str] = "LATISS/raw/all",
    desturiprefix: str = "tests/data/",
):
    # need to check if datasettype is a single str,
    # make it iterable
    iterable_datasettype = utils.iteration.ensure_iterable(datasettype)
    iterable_collections = utils.iteration.ensure_iterable(collections)

    # Run the package
    subprocess.run(
        [
            "python",
            "../src/move_embargo_args.py",
            temp_from,
            temp_to,
            "LATISS",
            "--embargohours",
            str(embargo_hours),
            "--datasettype",
            *iterable_datasettype,
            "--collections",
            # "LATISS/raw/all",
            *iterable_collections,
            "--nowtime",
            now_time_embargo,
            "--move",
            move,
            "--log",
            log,
            "--desturiprefix",
            desturiprefix,
        ],
        check=True,
    )
    # first test stuff in the temp_to butler
    butler_to = Butler(temp_to)
    registry_to = butler_to.registry
    counter = 0
    for dtype in datasettype:
        if any(
            dim in ["exposure", "visit"]
            for dim in registry_to.queryDatasetTypes(dtype)[0].dimensions.names
        ):
            print("dtype with exposure or visit info: ", dtype)
            ids_in_temp_to = [
                dt.dataId.mapping["exposure"]
                for dt in registry_to.queryDatasets(datasetType=..., collections=...)
            ]
        else:
            print("dtype with no exposure", dtype)
            datasetRefs = registry_to.queryDatasets(
                datasetType=datasettype, collections=collections
            )
            ids_in_temp_to = [dt.id for dt in datasetRefs]

        # verifying the contents of the temp_to butler
        # check that what we expect to move (ids_should_be_moved)
        # are in the temp_to repo (ids_in_temp_to)
        assert sorted(ids_should_be_moved) == sorted(
            ids_in_temp_to
        ), f"{ids_should_be_moved} should be in {temp_to} repo but isnt :(, \
            what is in it is: {ids_in_temp_to}"
        # now check the temp_from butler and see what remains
        butler_from = Butler(temp_from)
        registry_from = butler_from.registry

        if any(
            dim in ["exposure", "visit"]
            for dim in registry_to.queryDatasetTypes(dtype)[0].dimensions.names
        ):
            ids_in_temp_from = [
                dt.dataId.mapping["exposure"]
                for dt in registry_from.queryDatasets(datasetType=..., collections=...)
            ]
        else:
            ids_in_temp_from = [
                dt.id
                for dt in registry_from.queryDatasets(datasetType=..., collections=...)
            ]

        # verifying the contents of the from butler
        # if move is on, only the ids_remain should be in temp_from butler
        if move == "True":
            # checking that everything in temp_from butler
            # is in the ids_remain list
            assert sorted(ids_in_temp_from) == sorted(
                ids_should_remain_after_move
            ), f"move is {move} and {ids_in_temp_from} does not match what should be in \
                {temp_from}, which is {ids_should_remain_after_move}"
        # otherwise, if copy
        else:
            # everything in temp_from should be either
            # in ids_remain or ids_moved
            assert sorted(ids_in_temp_from) == sorted(
                ids_should_remain_after_move + ids_should_be_moved
            ), f"move is {move} and {ids_in_temp_from} should be in either \
                    {temp_from} or {temp_to} repo but it isn't"
        counter += 1
    assert (
        counter != 0
    ), f"Never went through the for loop shame on you, counter = {counter}"


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
        self.log = "True"

    def tearDown(self):
        """
        Removes all test files created by tests
        """
        shutil.rmtree(self.temp_dir.name, ignore_errors=True)

    # test the other datatypes:
    # first goodseeingdeepcoadd
    def test_raw_datatypes(self):
        """
        Test that move_embargo_args runs for a list
        of input datatypes
        """
        move = "False"
        # now_time_embargo = "now"
        # embargo_hours =  80.0 # hours
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = 0.1  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            # datasettype=["raw", "raw"],
            # collections=["LATISS/raw/all", "LATISS/raw/all"],
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    # test the other datatypes:
    # first goodseeingdeepcoadd

    def test_raw_datatypes_should_fail(self):
        """
        Test that move_embargo_args runs for a list
        of input datatypes
        """
        move = "False"
        # now_time_embargo = "now"
        # embargo_hours =  80.0 # hours
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = 0.1  # hours
        # IDs that should be moved to temp_to:
        # lol 2019111300059 should be in the ids_moved
        # list but I'm removing it to make sure the assertions
        # fail
        ids_moved = [
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

        try:
            is_it_there(
                embargo_hours,
                now_time_embargo,
                ids_remain,
                ids_moved,
                self.temp_from_path,
                self.temp_to_path,
                move=move,
                log=self.log,
                datasettype=["raw"],
                collections=["LATISS/raw/all"],
                desturiprefix=self.temp_dest_ingest,
            )
        except AssertionError:
            # At least one assertion failed, which is what we want
            pass
        else:
            # All assertions passed, so we raise a custom exception
            raise AtLeastOneAssertionFailedError(
                "All assertions within is_it_there passed and they should have failed"
            )

    def test_nothing_moves(self):
        """
        Nothing should move when the embargo hours falls right on
        the oldest exposure
        """
        move = "False"
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = 5596964.255774 / 3600.0
        # IDs that should be moved to temp_to:
        ids_moved = []
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )

    def test_after_now_01(self):
        """
        Verify that exposures after now are not being moved
        when the nowtime is right in the middle of the exposures
        """
        move = "False"
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = 0.1  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )

    def test_after_now_05(self):
        """
        Verify that exposures after now are not being moved
        when the nowtime is right in the middle of the exposures
        for a slightly longer embargo period (0.5 hours)
        """
        move = "False"
        now_time_embargo = "2020-01-17 16:55:11.322700"
        embargo_hours = 0.5  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_move(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "False"
        now_time_embargo = "2020-03-01 23:59:59.999999"
        embargo_hours = 3827088.677299 / 3600  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_copy(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "False"
        now_time_embargo = "2020-03-01 23:59:59.999999"
        embargo_hours = 3827088.677299 / 3600  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_move_midnight(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "False"
        now_time_embargo = "2020-03-02 00:00:00.000000"
        embargo_hours = 3827088.6773 / 3600  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_copy_midnight(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "False"
        now_time_embargo = "2020-03-02 00:00:00.000000"
        embargo_hours = 3827088.6773 / 3600  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_move_midnight_precision(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "False"
        now_time_embargo = "2020-03-02 00:00:00.000000"
        embargo_hours = 3827088.677301 / 3600  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_copy_midnight_precision(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "False"
        now_time_embargo = "2020-03-02 00:00:00.000000"
        embargo_hours = 3827088.677301 / 3600  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
            log=self.log,
            desturiprefix=self.temp_dest_ingest,
        )


if __name__ == "__main__":
    unittest.main()
