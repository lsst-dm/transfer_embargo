import unittest
from lsst.daf.butler import Butler
import subprocess


class TestMoveEmbargoArgs(unittest.TestCase):
    def test_populate(self):
        """
        Dual purpose;
        Makes the subprocess call that populates the fake_from
        static butler and checks it at the same time
        """
        # TODO enter times or IDs that we need to move
        # I'm guessing everything will be in ID format
        # TODO script to populate fake_from butler
        # might be called something different than the following:
        ID_list = ['blah']  # TODO
        dest = '/home/r/rnevin/transfer_embargo/tests/data/fake_from'
        subprocess.call(['python', '../src/tests/populate_test_butler.py',
                         '-f', '/repo/embargo',
                         '-t', dest,
                         '-m', center_time_populate_test_1, center_time_populate_test_2,
                         '-d', str(populate_test_days)])
        # Test our expectation of IDs that should be in the fake_from butler
        butler = Butler(dest)
        registry = butler.registry
        id_list = []
        for i, dt in enumerate(registry.queryDatasets(datasetType=...,
                                                      collections=...)):
            id_list.append(dt.id)
        for ID in ID_list:
            assert ID in id_list, f"{ID} missing from list of items in {dest} repo"
        # Also test if theres anything there that shouldnt be:
        for id_in in id_list:
            assert id_in in ID_list, f"{id_in} is in {dest} repo but is not in list to be moved"

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
        for i, dt in enumerate(registry.queryDatasets(datasetType=...,
                                                      collections=...)):
            id_in.append(dt.id)
        for ID in ids_moved:
            assert ID in id_in, f"{ID} should be in {fake_to} repo but isnt :("
        for ID in id_in:
            assert ID in ids_moved, f"{ID} should not be in {fake_to} repo but it is"
        # Now do the same for the fake_from butler
        butler = Butler(fake_from)
        registry = butler.registry
        id_in = []
        for i, dt in enumerate(registry.queryDatasets(datasetType=...,
                                                      collections=...)):
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
