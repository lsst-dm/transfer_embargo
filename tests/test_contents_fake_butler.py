import unittest
from lsst.daf.butler import Butler


class TestContents(unittest.TestCase):
    def test_fake_to(self):
        """
        Test the times of the files in the fake_to butler
        """
        # Probably will have to run the main program in 
        # order to do the moving, using a static 'now' time
        '''
        args = ['-f', '/repo/embargo',
                '-t', '/home/j/jarugula/scratch',
                '--instrument', 'LATISS',
                '-days', '30',
                '--datasettype', 'raw',
                '--collections', 'LATISS/raw/all',
                '--band', 'g']
        parser = move_embargo_args.parser(args)
        self.assertTrue(parser.long)
        '''
        # Is there an 'is in' assert? That might be nice
        namespace = 'fake_to'
        butler = Butler(namespace)
        registry = butler.registry
        
        print('running through')
        
        for i, dt in enumerate(registry.queryDimensionRecords('exposure')):
            end_time = dt.timespan.end
            print(end_time)
        #print(list_butler)
        
        


if __name__ == '__main__':
    unittest.main()
