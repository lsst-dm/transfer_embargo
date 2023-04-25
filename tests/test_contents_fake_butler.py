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
        # First step is to populate the fake test butler
        # I've only gotten this to work using subprocess
        subprocess.call(['python', 'populate_test_butler.py', '-f','/repo/embargo','-t','fake_from','-m','[2022-01-31T,2022-04-18]','-d', '35'])
        butler = Butler('/repo/embargo')
        registry = butler.registry
        for i, dt in enumerate(registry.queryDimensionRecords('exposure')):
            end_time = dt.timespan.end
            print(end_time)     
        print('done printing times')
        
        # Run the move_embargo_args code
        # with transfer='move' as an option in move_embargo_args.py
        # remove the where clause from move_embargo_args.py and current code
        
        # Look through the times of everything 
        

if __name__ == '__main__':
    unittest.main()
