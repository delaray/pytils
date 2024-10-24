# ****************************************************************
# Unit Tests for utils.py
# ****************************************************************
#
# CLI: python -m unittest tests/test_data.py
# CLI: coverage run -m unittest
# CLI: coverage report
#
# ****************************************************************

import os
import unittest
import pandas as pd

# Project imports
from utils.data import split_list, split_dict, split_dataframe

class TestData(unittest.TestCase):

    def test_split_list(self):

        # Odd length list
        mylist = [1, 2, 3, 4, 5]
        pieces1 = split_list(mylist, n=2)
        self.assertEqual(pieces1[0], [1, 2, 3])
        self.assertEqual(pieces1[1], [4, 5])

        pieces2 = split_list(mylist, n=3)
        self.assertEqual(pieces2[0], [1, 2])
        self.assertEqual(pieces2[1], [3, 4])
        self.assertEqual(pieces2[2], [5])

        pieces3 = split_list(mylist, n=6)
        self.assertEqual(pieces3[0], mylist)


    def test_split_dict(self):

        d = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5}
        pieces1 = split_dict(d, n=2)
        self.assertEqual(len(pieces1), 2)
        self.assertEqual(pieces1[0], {'a': 1, 'c': 3, 'e': 5})
        self.assertEqual(pieces1[1], {'b': 2, 'd': 4})

        pieces2 = split_dict(d, n=3)
        self.assertEqual(len(pieces2), 3)
        self.assertEqual(pieces2[0], {'a': 1, 'd': 4})
        self.assertEqual(pieces2[1], {'b': 2, 'e': 5})
        self.assertEqual(pieces2[2], {'c': 3})

        pieces3 = split_dict(d, n=6)
        self.assertEqual(len(pieces3), 1)
        self.assertEqual(pieces3[0], d)


    def test_split_dataframe(self):

        df = pd.DataFrame([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
                          columns=['Odd', 'Even'])
        pieces1 = split_dataframe(df, n=2)
        self.assertEqual(len(pieces1), 2)
        self.assertEqual(list(pieces1[0].loc[0]), [1, 2])
        self.assertEqual(list(pieces1[1].loc[4]), [9, 10])

        pieces2 = split_dataframe(df, n=6)
        self.assertEqual(len(pieces2), 1)
        self.assertEqual(list(pieces2[0].loc[0]), [1, 2])
        self.assertEqual(list(pieces2[0].loc[4]), [9, 10])

if __name__ == '__main__':
    unittest.main()


# ****************************************************************
# End of File
# ****************************************************************
