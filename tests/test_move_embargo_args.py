import unittest

import sys
sys.path.insert(0,"..")
from src import move_embargo_args

class TestSum(unittest.TestCase):
    def test_list_int(self):
        """
        Test something about move_embargo_args
        """
        args = [1, 2, 3] # need to insert arguments that go into move_embargo_args here to test it
        result = move_embargo_args(args)
        # I'm not sure what we need to assert (below)
        self.assertEqual(result, 6)

if __name__ == '__main__':
    unittest.main()
