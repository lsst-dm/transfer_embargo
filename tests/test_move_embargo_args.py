import subprocess
import unittest
import shutil
import os
import tempfile

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
):
    # Run the package
    subprocess.call(
        [
            "python",
            "../src/move_embargo_args.py",
            temp_from,
            temp_to,
            "LATISS",
            "--embargohours",
            str(embargo_hours),
            "--datasettype",
            "raw",
            "--collections",
            "LATISS/raw/all",
            "--nowtime",
            now_time_embargo,
            "--move",
            move,
            "--log",
            log,
        ]
    )
    # first test stuff in the temp_to butler
    butler_to = Butler(temp_to)
    registry_to = butler_to.registry
    ids_in_temp_to = [
        dt.dataId.full["exposure"]
        for dt in registry_to.queryDatasets(datasetType=..., collections=...)
    ]
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
    ids_in_temp_from = [
        dt.dataId.full["exposure"]
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
        # everything in temp_from should be either in ids_remain or ids_moved
        assert sorted(ids_in_temp_from) == sorted(
            ids_should_remain_after_move + ids_should_be_moved
        ), f"move is {move} and {ids_in_temp_from} should be in either \
                {temp_from} or {temp_to} repo but it isn't"


class TestMoveEmbargoArgs(unittest.TestCase):
    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory()
        temp_from_path = os.path.join(temp_dir.name, "temp_test_from")
        temp_to_path = os.path.join(temp_dir.name, "temp_test_to")
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
        # The above is if we are running 'move',
        # If copy, it should be both of these
        # added together
        self.log = "False"

    def tearDown(self):
        shutil.rmtree(self.temp_dir.name, ignore_errors=True)

    def test_main_move(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "True"
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
        )
        # os.system("sqlite3 "+self.temp_from_path+"/gen3.sqlite3")
'''
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
        )

    def test_main_move_midnight(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "True"
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
        )

    def test_main_move_midnight_precision(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "True"
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
        )
'''

if __name__ == "__main__":
    unittest.main()
