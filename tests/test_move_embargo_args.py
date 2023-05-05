import unittest
# import sys
# sys.path.insert(0, "..")
# from src import move_embargo_args
import move_embargo_args


class TestEmbargoArgs(unittest.TestCase):
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
