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
        subprocess.call(['python', 'populate_test_butler.py', '-f','/repo/embargo','-t','fake_from','-m','2022-01-31T00:00:00.000','2022-04-18T00:00:00.000','-d', '100'])
        #,2022-04-18T00:00:00.000
        
        # Then test to see what is in there
        butler = Butler('fake_from')
        registry = butler.registry
        for i, dt in enumerate(registry.queryDatasets(datasetType=None,
                                                          collections=None)):
            end_time = dt.timespan.end
            print(end_time)     
        print('done printing times')
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        #for i, dt in enumerate(registry.queryDimensionRecords('exposure')):
        #    end_time = dt.timespan.end
        #    print(end_time)     
        STOP
        
        # Then test to see what is in there
        butler = Butler('fake_from')
        registry = butler.registry
        for i, dt in enumerate(registry.queryDimensionRecords('exposure')):
            end_time = dt.timespan.end
            print(end_time)     
        print('done printing times')
        STOP
        
        
        
        # Run the move_embargo_args code
        # with transfer='move' as an option in move_embargo_args.py
        # remove the where clause from move_embargo_args.py and current code
        subprocess.call(['python', '../src/move_embargo_args.py', '-f', 'fake_from', '-t', 'fake_to', 
                         '-d', '5', '--instrument', 'LATISS', '--datasettype', 'raw', 
                         '--collections', 'LATISS/raw/all', '--band', 'g', '--nowtime', '2022-01-31T00:00:00.000'])
        # change move_embargo_args.py
        
        
        # Look through the times of everything 
        
        butler = Butler('fake_to')
        registry = butler.registry
        for i, dt in enumerate(registry.queryDimensionRecords('exposure')):
            end_time = dt.timespan.end
            print(end_time)     
        print('done printing times')
        
        
        

if __name__ == '__main__':
    unittest.main()
