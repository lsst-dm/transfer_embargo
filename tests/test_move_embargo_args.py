import subprocess
import unittest
import shutil
import os
import tempfile

from lsst.daf.butler import Butler


def is_it_there(
    embargo_hours: float,
    now_time_embargo: str,
    ids_remain,
    ids_moved,
    test_from,
    test_to,
    move,
):
    # Run the package
    subprocess.call(
        [
            "python",
            "../src/move_embargo_args.py",
            "-f",
            test_from,
            "-t",
            test_to,
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
    # Things to check about what is in there:
    # 1) If stuff is in fake_to that should be there
    # 2) If stuff is in fake_from that should be there
    # 3) If stuff remains in fake_from
    # 4) If wrong stuff was moved to fake_to
    # ^ We will have two modes of testing, one where
    # the files are copied and one where they are
    # moved

    # First test stuff in the fake_to butler
    butler_to = Butler(test_to)
    registry_to = butler_to.registry
    id_in_to = [
        dt.dataId.full["exposure"]
        for dt in registry_to.queryDatasets(datasetType=..., collections=...)
    ]
    print(id_in_to)

    for ID in ids_moved:
        assert ID in id_in_to, f"{ID} should be in {test_to} repo but isnt :("
    for ID in id_in_to:
        assert ID in ids_moved, f"{ID} should not be in {test_to} repo but it is"

    # Now do the same for the test_from butler
    butler_from = Butler(test_from)
    registry_from = butler_from.registry
    id_in_from = [
        dt.dataId.full["exposure"]
        for dt in registry_from.queryDatasets(datasetType=..., collections=...)
    ]
    print(id_in_from)

    if move == "True":
        for ID in id_in_from:
            assert ID in ids_remain, f"{ID} should not be in {test_from} repo but it is"
        for ID in ids_remain:
            assert (
                ID in id_in_from + id_in_to
            ), f"{ID} should not be in {test_from} repo but it is"
    else:
        for ID in id_in_from:
            assert ID in ids_remain, f"{ID} should be in {test_from} repo but it isn't"
        for ID in ids_remain:
            assert ID in id_in_from, f"{ID} should be in {test_from} repo but it isn't"


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
        # IDs that should be moved:
        ids_moved = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2020011700004,
        ]
        # IDs that should stay in the fake_from:
        ids_remain = [
            2019111300059,
            2019111300061,
            2020011700002,
            2020011700003,
            2020011700004,
            2020011700005,
            2020011700006,
        ]

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

    # def test_time_format_input(self):
    #     test_from = self.temp_from_path
    #     test_to = self.temp_to_path
    #     ids_moved = [
    #        2019111300059,
    #        2019111300061,
    #        2020011700002,
    #        2020011700003,
    #        2020011700004,
    #     ]
    #     # IDs that should stay in the fake_from:
    #     ids_remain = [
    #        2019111300059,
    #        2019111300061,
    #        2020011700002,
    #        2020011700003,
    #        2020011700004,
    #        2020011700005,
    #        2020011700006,
    #     ]
    #     with self.assertRaises(ValueError):
    #         is_it_there(80.0, "2019111300059", ids_remain, ids_moved, test_from, test_to, move="False")


if __name__ == "__main__":
    unittest.main()
