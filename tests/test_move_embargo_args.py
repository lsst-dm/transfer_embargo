import unittest
from lsst.daf.butler import Butler
import subprocess


class TestMoveEmbargoArgs(unittest.TestCase):
    def test_main(self):
        """
        Run move_embargo_args to move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        now_time_embargo = '2022-09-14T00:00:00.000'  # TODO, this is a fixed now
        fake_from = '/home/r/rnevin/transfer_embargo/tests/data/fake_from'
        fake_to = '/home/r/rnevin/transfer_embargo/tests/data/fake_to'
        # IDs that should be moved:
        ids_moved = ['blah']
        # IDs that should stay in the fake_from:
        ids_remain = ['blah']
        # Now run the package
        subprocess.call(['python', '../src/move_embargo_args.py',
                         '-f', fake_from,
                         '-t', fake_to,
                         '--embargohours', 80, '--instrument', 'LATISS',
                         '--datasettype', 'raw',
                         '--collections', 'LATISS/raw/all',
                         '--nowtime', now_time_embargo])
        # Things to check about what is in there:
        # 1) If stuff is in fake_to that should be there
        # 2) If stuff is in fake_from that should be there
        # 3) If stuff remains in fake_to
        # 4) If wrong stuff was moved to fake_from
        # 5) Could also test length of list of items but might be redundant
        # 6) Could also test exact time on files

        # First test stuff in the fake_to butler
        butler = Butler(fake_to)
        registry = butler.registry
        id_in = []
        for dt in registry.queryDatasets(datasetType=...,
                                         collections=...):
            id_in.append(dt.id)
        for ID in ids_moved:
            assert ID in id_in, f"{ID} should be in {fake_to} repo but isnt :("
        for ID in id_in:
            assert ID in ids_moved, f"{ID} should not be in {fake_to} repo but it is"
        # Now do the same for the fake_from butler
        butler = Butler(fake_from)
        registry = butler.registry
        id_in = []
        for dt in registry.queryDatasets(datasetType=...,
                                         collections=...):
            id_in.append(dt.id)
        for ID in ids_remain:
            assert ID in id_in, f"{ID} should be in {fake_from} repo but isnt :("
        for ID in id_in:
            assert ID in ids_remain, f"{ID} should not be in {fake_from} repo but it is"

    def test_parser(self):
        """
        Test the parser without having to execute the code
        """
        args = ['-f', '/repo/embargo',
                '-t', '/home/j/jarugula/scratch',
                '--instrument', 'LATISS',
                '--embargohours', '80.0',
                '--datasettype', 'raw',
                '--collections', 'LATISS/raw/all']
        parser = move_embargo_args.parser(args)
        self.assertTrue(parser.long)
        '''

        #python move_embargo_args.py --help
        #python move_embargo_args.py -fromrepo $arg_fromrepo
            -torepo $arg_torepo
            -instrument $arg_instrument
            -days $arg_days
            -dtype $arg_dtype
        self.assertRaises(move_embargo_args())
        self.assertRaises(move_embargo_args("-fromrepo" = arg_fromrepo,
                                            "-torepo" = arg_torepo,
                                            "-instrument" = arg_instrument,
                                            "--embargohours" = arg_hours,
                                            "-dtype" = arg_dtype))
        '''


if __name__ == '__main__':
    unittest.main()
