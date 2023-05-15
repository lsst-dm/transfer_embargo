import unittest
from lsst.daf.butler import Butler
import subprocess


class TestMoveEmbargoArgs(unittest.TestCase):
    def test_main(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        MOVED = False  # false if copy
        now_time_embargo = '2020-03-01 23:59:59.999999'  # TODO, this is a fixed now
        embargo_hours = 3827088.677299/3600  # hours
        test_from = '/home/r/rnevin/transfer_embargo/tests/data/test_from'
        test_to = '/home/r/rnevin/transfer_embargo/tests/data/test_to'

        # clear test butler:
        # TEMPORARY
        # butler = Butler(test_to, writeable=True)
        # butler.pruneCollection('LATISS/raw/all', purge=True, unstore=True)

        # IDs that should be moved:
        ids_moved = [2019111300059,
                     2019111300061,
                     2020011700002,
                     2020011700003,
                     2020011700004]
        # IDs that should stay in the fake_from:
        ids_remain = [2019111300059,
                      2019111300061,
                      2020011700002,
                      2020011700003,
                      2020011700004,
                      2020011700005,
                      2020011700006]
        # Now run the package
        subprocess.call(['python', '../src/move_embargo_args.py',
                         '-f', test_from,
                         '-t', test_to,
                         '--embargohours', str(embargo_hours), '--instrument', 'LATISS',
                         '--datasettype', 'raw',
                         '--collections', 'LATISS/raw/all',
                         '--nowtime', now_time_embargo])
        # Things to check about what is in there:
        # 1) If stuff is in fake_to that should be there
        # 2) If stuff is in fake_from that should be there
        # 3) If stuff remains in fake_from
        # 4) If wrong stuff was moved to fake_to
        # ^ We will have two modes of testing, one where
        # the files are copied and one where they are
        # moved

        # First test stuff in the fake_to butler
        butler = Butler(test_to)
        registry = butler.registry
        id_in = [dt.dataId.full['exposure'] for
                 dt in registry.queryDatasets(datasetType=..., collections=...)]
        for ID in ids_moved:
            assert ID in id_in, f"{ID} should be in {test_to} repo but isnt :("
        for ID in id_in:
            assert ID in ids_moved, f"{ID} should not be in {test_to} repo but it is"

        # Now do the same for the test_from butler
        butler = Butler(test_from)
        registry = butler.registry
        id_in = [dt.dataId.full['exposure'] for
                 dt in registry.queryDatasets(datasetType=..., collections=...)]
        for ID in ids_remain:
            assert ID in id_in, f"{ID} should be in {test_from} repo but isnt :("
        if MOVED:
            for ID in id_in:
                assert ID in ids_remain, f"{ID} should not be in {test_from} repo but it is"


if __name__ == '__main__':
    unittest.main()
