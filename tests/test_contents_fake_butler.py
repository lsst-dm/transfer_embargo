import unittest
from lsst.daf.butler import Butler
import move_embargo_args
import populate_test_butler
import subprocess


class TestContents(unittest.TestCase):
    def test_fake_to(self):
        """
        Test the times of the files in the fake_to butler
        """
        
        
        # Zeroth step is to prune the fake repos:
        for repo in ['fake_from','fake_to']:
            butler = Butler(repo)
            registry = butler.registry
            # There's gotta be a better way to check if this registry is empty
            time_list = []
            for i, dt in enumerate(registry.queryDimensionRecords('exposure')):
                end_time = dt.timespan.end
                time_list.append(end_time)
            if time_list is None:
                print('nothing here', repo)
            else:
              
                print('pruning', repo)
                # Then prune
                # We also need to fix this
                dest = Butler(repo, writeable=True)
                collections = 'LATISS/raw/all'
                dest.pruneDatasets('raw',purge=True, unstore=True)
        
        
        # Now populate the fake_from butler
        # I've only gotten this to work using subprocess
        subprocess.call(['python', 'populate_test_butler.py', '-f','/repo/embargo','-t','fake_from','-m','[2022-01-31,2022-04-18]','-d', '10'])
        
        
        # Run the move_embargo_args code
        # with transfer='move' as an option in move_embargo_args.py
        # remove the where clause from move_embargo_args.py and current code
        subprocess.call(['python', 'move_embargo_args.py', '-f', 'fake_from', '-t', 'fake_to', 
                         '-d', '5', '--instrument', 'LATISS', '--datasettype', 'raw', 
                         '--collections', 'LATISS/raw/all', '--band', 'g', '--nowtime', '2022-01-31'])
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
