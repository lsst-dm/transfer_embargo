import astropy.time
import unittest
from lsst.daf.butler import Butler
import subprocess
import numpy as np


class TestEmbargoArgs(unittest.TestCase):
    def test_populate(self):
        """
        Dual purpose;
        Makes the subprocess call that populates the fake_from
        static butler and checks it at the same time
        """
        # TODO enter times or IDs that we need to move
        # TODO might be called something different than populate
        ID_list = ['blah']
        dest = '/home/r/rnevin/transfer_embargo/tests/data/fake_from''
        subprocess.call(['python', '../src/tests/populate_test_butler.py',
                         '-f', '/repo/embargo',
                         '-t', dest,
                         '-m', center_time_populate_test_1, center_time_populate_test_2,
                         '-d', str(populate_test_days)])
        # Test that what we say is there is there
        butler = Butler(repo)
        registry = butler.registry
        id_list = []
        for i, dt in enumerate(registry.queryDatasets(datasetType=...,
                                                      collections=...)):
            id_list.append(dt.id)
        for ID in ID_list:
            assert ID in id_list, f"{ID} missing from list of items in {dest} repo"
        
    def test_main(self):
        """
        Move some IDs from the fake_from butler
        to the fake_to butler and test which ones moved
        """
        now_time_embargo = '2022-09-14T00:00:00.000'
        subprocess.call(['python', '../src/move_embargo_args.py',
                         '-f', '/home/r/rnevin/transfer_embargo/tests/data/fake_from',
                         '-t', '/home/r/rnevin/transfer_embargo/tests/data/fake_to',
                         '--embargohours', 80, '--instrument', 'LATISS',
                         '--datasettype', 'raw',
                         '--collections', 'LATISS/raw/all',
                         '--nowtime', now_time_embargo])
        # Things to check about what is in there:
        # 1) If stuff is in fake_to that should be there
        # 2) If stuff is in fake_from that should be there
        # 3) If stuff remains in fake_to
        # 4) If wrong stuff was moved to fake_from
        
        # how to check what is there?
        for repo in ['/home/r/rnevin/transfer_embargo/tests/data/fake_from',
                     '/home/r/rnevin/transfer_embargo/tests/data/fake_to']:
            butler = Butler(repo)
            registry = butler.registry
            # There's gotta be a better way to check if this registry is empty
            id_list = []
            for i, dt in enumerate(registry.queryDatasets(datasetType=...,
                                                          collections=...)):
                id_list.append(dt.id)
            assert len(id_list) == 0, "prune failed"
        self.assertEqual(np.shape(time), np.shape(output))
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
