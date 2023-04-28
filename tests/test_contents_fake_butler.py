import unittest
from lsst.daf.butler import Butler
import move_embargo_args
import populate_test_butler
import subprocess


class TestContents(unittest.TestCase):
    def test_fake_to(self):
        """
        Test the times of the files in the fake_to butler moved by our program
        """
        # Info about things you are populating the fake_from butler with
        # including the window (+/-) of files you're moving there
        # and the center times of the file windows
        populate_test_days = 35
        center_time_populate_test_1 = '2022-04-18T00:00:00.000'
        center_time_populate_test_2 = '2022-01-31T00:00:00.000'
        center_time_populate_list = [center_time_populate_test_1, center_time_populate_test_2]
        
        # Info about the files that will be moved using the package
        # from the fake_from to the fake_to butler
        # move_embargo_args SHOULD move files that are five days before
        # the now_time_embargo to the fake_to butler
        embargo_days = 30
        now_time_embargo = '2022-01-31T00:00:00.000'
        
        
        # First step is to remove/prune the data in the fake repos:
        for repo in ['fake_from','fake_to']:
            butler = Butler(repo)
            registry = butler.registry
            # There's gotta be a better way to check if this registry is empty
            time_list = []
            for i, dt in enumerate(registry.queryDatasets(datasetType=None,
                                                          collections=None)):
                end_time = dt.timespan.end
                time_list.append(end_time)
                print(end_time)
            if len(time_list) == 0:
                print(f'nothing in the {repo} repo')
            else:
              
                print(f'pruning the {repo} repo')
                dest = Butler(repo, writeable=True)
                collections = 'LATISS/raw/all'
                datasetType = 'raw'
                dest.pruneDatasets(registry.queryDatasets(datasetType=None,
                                                          collections=None),
                                   purge=True, unstore=True)
        
        # Now populate the fake_from butler
        # using populate_test_butler
        subprocess.call(['python', 'populate_test_butler.py', '-f','/repo/embargo','-t','fake_from','-m', center_time_populate_test_1, center_time_populate_test_2, '-d', str(populate_test_days)])
        #,2022-04-18T00:00:00.000
        
        
        # Run the move_embargo_args code
        # with transfer='move' as an option in move_embargo_args.py
        # remove the where clause from move_embargo_args.py and current code
        subprocess.call(['python', '../src/move_embargo_args.py', '-f', 'fake_from', '-t', 'fake_to', 
                         '-d', str(embargo_days), '--instrument', 'LATISS', '--datasettype', 'raw', 
                         '--collections', 'LATISS/raw/all', '--band', 'g', '--nowtime', now_time_embargo])
        # change move_embargo_args.py
        
        
        # Then test to see what is in the new butler ('fake_to')
        # It should be 
        butler = Butler('fake_to')
        registry = butler.registry
        time_list = []
        for i, dt in enumerate(registry.queryDatasets(datasetType=None,
                                                          collections=None)):
            end_time = dt.timespan.end
            time_list.append(end_time)
        # Test if any of these are 
        window_populate = astropy.time.TimeDelta(populate_test_days, format='jd')
        window_embargo = astropy.time.TimeDelta(window_days, format='jd')
        # First test that the populate code works
        for center_time in center_time_populate_list:
            assert time_list.any() <  center_time - window_populate, \
                "populate_test_butler failed, there are files before the window"
            assert time_list.any() >  center_time + window_populate, \
                "populate_test_butler failed, there are files after the window"
        # Now test if move_embargo_args works
        assert time_list.any() > now_time_embargo - window_embargo, \
            "you moved files that are still under embargo"
        
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        
        
        

if __name__ == '__main__':
    unittest.main()
