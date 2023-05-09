import astropy.time
import unittest
from lsst.daf.butler import Butler
import subprocess
import numpy as np


class TestContents(unittest.TestCase):
    def test_fake_to(self):
        """
        Test the times of the files in the fake_to butler moved by our program
        """
        # Info about things you are populating the fake_from butler with
        # including the window (+/-) of files you're moving there
        # and the center times of the file windows
        populate_test_days = 5
        center_time_populate_test_1 = '2022-09-14T00:00:00.000'
        center_time_populate_test_2 = '2022-10-31T00:00:00.000'
        center_time_populate_list = [center_time_populate_test_1, center_time_populate_test_2]
        # Info about the files that will be moved using the package
        # from the fake_from to the fake_to butler
        # move_embargo_args SHOULD move files that are five days before
        # the now_time_embargo to the fake_to butler
        embargo_days = 1
        now_time_embargo = '2022-09-14T00:00:00.000'
        # First step is to remove/prune the data in the fake repos:
        for repo in ['/home/r/rnevin/transfer_embargo/tests/fake_from',
                     '/home/r/rnevin/transfer_embargo/tests/fake_to']:
            butler = Butler(repo)
            registry = butler.registry
            # There's gotta be a better way to check if this registry is empty
            id_list = []
            for i, dt in enumerate(registry.queryDatasets(datasetType=..., 
                                                          collections=...)):
                id_list.append(dt.id)
            if len(id_list) == 0:
                print(f'nothing in the {repo} repo')
            else:
                print(f'pruning the {repo} repo')
                dest = Butler(repo, writeable=True)
                # pruning data points
                # maybe dest.prune_datasets()
                dest.pruneDatasets(registry.queryDatasets(datasetType=..., 
                                                          collections=...),
                                   purge=True, unstore=True)
        # Check that the prune worked
        for repo in ['/home/r/rnevin/transfer_embargo/tests/fake_from',
                     '/home/r/rnevin/transfer_embargo/tests/fake_to']:
            butler = Butler(repo)
            registry = butler.registry
            # There's gotta be a better way to check if this registry is empty
            id_list = []
            for i, dt in enumerate(registry.queryDatasets(datasetType=..., 
                                                          collections=...)):
                id_list.append(dt.id)
            assert len(id_list) ==  0, "prune failed"
        # Now populate the fake_from butler
        # using populate_test_butler
        subprocess.call(['python', 'populate_test_butler.py',
                         '-f', '/repo/embargo',
                         '-t', '/home/r/rnevin/transfer_embargo/tests/fake_from',
                         '-m', center_time_populate_test_1, center_time_populate_test_2,
                         '-d', str(populate_test_days)])
        # Check that the right number of files were moved
        butler = Butler('/home/r/rnevin/transfer_embargo/tests/fake_from')
        registry = butler.registry
        # There's gotta be a better way to check if this registry is empty
        id_list = []
        for i, dt in enumerate(registry.queryDatasets(datasetType=..., 
                                                      collections=...)):
            ids = dt.id
            id_list.append(ids)
        print(repo, len(id_list))
        # Run the move_embargo_args code
        # with transfer='move' as an option in move_embargo_args.py
        # remove the where clause from move_embargo_args.py and current code
        subprocess.call(['python', '../src/move_embargo_args.py',
                         '-f', '/home/r/rnevin/transfer_embargo/tests/fake_from',
                         '-t', '/home/r/rnevin/transfer_embargo/tests/fake_to',
                         '-d', str(embargo_days), '--instrument', 'LATISS',
                         '--datasettype', 'raw',
                         '--collections', 'LATISS/raw/all',
                         '--band', 'g', '--nowtime', now_time_embargo])
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Then test to see what is in the new butler ('fake_to')
        butler = Butler('fake_to')
        registry = butler.registry
        time_list = []
        for i, dt in enumerate(registry.queryDatasets(datasetType=None,
                                                      collections=None)):
            end_time = dt.timespan.end
            time_list.append(end_time)
        window_populate = astropy.time.TimeDelta(populate_test_days, format='jd')
        window_embargo = astropy.time.TimeDelta(embargo_days, format='jd')
        # First test that the populate code works
        for center_time in center_time_populate_list:
            print('time list', time_list)
            assert np.array(time_list).any() < (center_time - window_populate), \
                "populate_test_butler failed, there are files before the window"
            assert time_list.any() > center_time + window_populate, \
                "populate_test_butler failed, there are files after the window"
        # Now test if move_embargo_args works
        assert time_list.any() > now_time_embargo - window_embargo, \
            "you moved files that are still under embargo"
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


if __name__ == '__main__':
    unittest.main()
