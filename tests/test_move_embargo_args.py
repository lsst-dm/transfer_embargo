import os
import shutil
import subprocess
import tempfile
import unittest

import lsst.utils as utils
import pytest
from lsst.daf.butler import Butler


def is_it_there(
    embargo_hours: float,
    now_time_embargo: str,
    ids_should_remain_after_move,
    ids_should_be_moved,
    temp_from,
    temp_to,
    move=None,
    log: str = "INFO",
    datasettype: list | str = "raw",
    collections: list | str = "LATISS/raw/all",
    desturiprefix: str = "tests/data/",
):
    # need to check if datasettype is a single str,
    # make it iterable
    iterable_datasettype = utils.iteration.ensure_iterable(datasettype)
    iterable_collections = utils.iteration.ensure_iterable(collections)
    # Run the package
    print("this is the move arg", str(move) if move is not None else "")
    subprocess_args = [
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
        *iterable_collections,
        "--nowtime",
        now_time_embargo,
        "--log",
        log,
        "--desturiprefix",
        desturiprefix,
    ]

    # add --move argument only if move is not None
    if move is not None:
        subprocess_args.extend(["--move", str(move)])
    assert move is None, f"move is {move}"

    # now run the subprocess
    subprocess.run(subprocess_args, check=True)

    # first test stuff in the temp_to butler
    butler_to = Butler(temp_to)
    registry_to = butler_to.registry
    counter = 0
    for dtype in datasettype:
        try:
            if any(
                dim in ["exposure", "visit"]
                for dim in registry_to.queryDatasetTypes(dtype)[0].dimensions.names
            ):
                ids_in_temp_to = [
                    dt.dataId.mapping["exposure"]
                    for dt in registry_to.queryDatasets(
                        datasetType=..., collections=...
                    )
                ]
            else:
                datasetRefs = registry_to.queryDatasets(
                    datasetType=datasettype, collections=collections
                )
                ids_in_temp_to = [dt.id for dt in datasetRefs]
        except IndexError:
            ids_in_temp_to = []

        # verifying the contents of the temp_to butler
        # check that what we expect to move (ids_should_be_moved)
        # are in the temp_to repo (ids_in_temp_to)
        sorted_moved = sorted(ids_should_be_moved)
        sorted_temp_to = sorted(ids_in_temp_to)
        assert (
            sorted_moved == sorted_temp_to
        ), f"{sorted_moved} should be in {temp_to} repo but is not, instead what is there: {sorted_temp_to}"
        # now check the temp_from butler and see what remains
        butler_from = Butler(temp_from)
        registry_from = butler_from.registry

        # first check if anything is in the registry_to:
        try:
            if any(
                dim in ["exposure", "visit"]
                for dim in registry_from.queryDatasetTypes(dtype)[0].dimensions.names
            ):
                ids_in_temp_from = [
                    dt.dataId.mapping["exposure"]
                    for dt in registry_from.queryDatasets(
                        datasetType=..., collections=...
                    )
                ]
            else:
                ids_in_temp_from = [
                    dt.id
                    for dt in registry_from.queryDatasets(
                        datasetType=..., collections=...
                    )
                ]
        except IndexError:
            ids_in_temp_from = []

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
        counter += 1
        counter += 1
    assert (
        counter != 0
    ), f"Never went through the for loop shame on you, \
       counter = {counter}"


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

    @pytest.mark.xfail(strict=True)
    def test_should_fail_if_move_is_true(self):
        """
        Move being true is scary because it deletes everything
        in the source repo. Let's make sure that move_embargo_args
        has a mechnism in place to fail if you set move to be true
        """
        move = "anything"
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
        embargo_hours = 0.1  # hours
        # IDs that should be moved to temp_to:
        ids_moved = [
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
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_nothing_moves(self):
        """
        Nothing should move when the embargo hours falls right on
        the oldest exposure
        """
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
            log=self.log,
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
            log=self.log,
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

        # try:
        is_it_there(
            embargo_hours,
            now_time_embargo,
            ids_remain,
            ids_moved,
            self.temp_from_path,
            self.temp_to_path,
            log=self.log,
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
            log=self.log,
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
            log=self.log,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_move_midnight(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
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
            log=self.log,
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
            log=self.log,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )

    def test_main_move_midnight_precision(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
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
            log=self.log,
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
            log=self.log,
            datasettype=["raw"],
            collections=["LATISS/raw/all"],
            desturiprefix=self.temp_dest_ingest,
        )


if __name__ == "__main__":
    unittest.main()
