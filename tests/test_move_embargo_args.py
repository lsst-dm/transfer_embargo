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
):
    # Run the package
    subprocess.call(
        [
            "python",
            "../src/move_embargo_args.py",
            "-f",
            temp_from,
            "-t",
            temp_to,
            "--embargohours",
            str(embargo_hours),
            "--instrument",
            "LATISS",
            "--datasettype",
            "raw",
            "--collections",
            "LATISS/raw/all",
            "--nowtime",
            now_time_embargo,
            "--move",
            move,
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
    for ID in ids_should_be_moved:
        assert ID in ids_in_temp_to, f"{ID} should be in {temp_to} repo but isnt :("
    # check that all ids currently in the temp_to butler (ids_in_temp_to)
    # are in what we expect to move (ids_should_be_moved)
    # this is different from the above because it will trigger if 
    # there is anything in temp_to that wasn't expected to be there
    # whereas the above will trigger if anything that
    # should have moved is not in there
    for ID in ids_in_temp_to:
        assert ID in ids_should_be_moved, f"{ID} should not be in {temp_to} repo but it is"

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
        # checking that everything in temp_from butler is in the ids_remain list
        for ID in ids_in_temp_from:
            assert ID in ids_should_remain_after_move, f"{ID} should not be in {test_from} repo but it is"
        # checking that ids_remain are still in the temp_from butler
        for ID in ids_should_remain_after_move:
            assert ID in ids_in_temp_from, f"{ID} should not be in {test_from} repo but it is"
            
            assert (
                ID in ids_in_temp_from + ids_in_temp_to
            ), f"{ID} should not be in {test_from} repo but it is"
    # otherwise, if copy
    else:
        # everything in temp_from should be either in ids_remain or ids_moved
        for ID in ids_in_temp_from:
            assert (ID in ids_should_remain_after_move + ids_should_be_moved), \
            f"{ID} should be in either {temp_from} or {temp_to} repo but it isn't"
        # conversely, everything in ids_remain and ids_moved should be in the temp_from butler
        for ID in (ids_should_remain_after_move + ids_should_be_moved):
            assert ID in ids_in_temp_from, f"{ID} should be in {temp_from} repo but it isn't"


class TestMoveEmbargoArgs(unittest.TestCase):
    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory()
        temp_from_path = os.path.join(temp_dir.name, "temp_test_from")
        temp_to_path = os.path.join(temp_dir.name, "temp_test_to")
        shutil.copytree("data/test_from", temp_from_path)
        shutil.copytree("data/test_to", temp_to_path)
        self.temp_dir = temp_dir
        self.temp_from_path = temp_from_path
        self.temp_to_path = temp_to_path

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
        # The above is if we are running 'move',
        # If copy, it should be both of these
        # added together

        self.now_time_embargo = now_time_embargo
        self.embargo_hours = embargo_hours
        self.ids_moved = ids_moved
        self.ids_remain = ids_remain

    def tearDown(self):
        shutil.rmtree(self.temp_dir.name, ignore_errors=True)

    def test_main_move(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "True"
        is_it_there(
            self.embargo_hours,
            self.now_time_embargo,
            self.ids_remain,
            self.ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
        )

    def test_main_copy(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        move = "False"
        is_it_there(
            self.embargo_hours,
            self.now_time_embargo,
            self.ids_remain,
            self.ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            move=move,
        )


if __name__ == "__main__":
    unittest.main()
