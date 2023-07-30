# ****************************************************************
# Unit Tests for utils.py
# ****************************************************************

import os
import unittest
import pandas as pd

# Project imports
from src.globals import KGCL_DIR
from src.utils import split_list
from src.utils import make_data_pathname
from src.utils import make_results_pathname
from src.utils import load_csv, save_csv


# CLI: python -m unittest tests/test_utils.py
# CLI: coverage run -m unittest
# CLI: coverage report

class TestUtils(unittest.TestCase):

    def test_split_list(self):
        mylist = [1, 2, 3, 4, 5]
        pieces = split_list(mylist, n=2)
        self.assertEqual(pieces[0], [1, 3])
        self.assertEqual(pieces[1], [2, 4])
        self.assertEqual(pieces[2], [5])
        
    def test_make_data_pathname(self):
        pathname = make_data_pathname('foobar.csv')
        self.assertEqual('data' in pathname, True)
        self.assertEqual(pathname.endswith('foobar.csv'), True)
        self.assertEqual(pathname.startswith(KGCL_DIR), True)

    def test_make_results_pathname(self):
        pathname = make_results_pathname('foobar.csv')
        self.assertEqual('results' in pathname, True)
        self.assertEqual(pathname.endswith('foobar.csv'), True)
        self.assertEqual(pathname.startswith(KGCL_DIR), True)

    def test_csv(self):
        df1 = pd.DataFrame([[1.0, 2.0], [3.0, 4.0]])
        filename = 'dataframe.csv'
        save_csv(df1, filename)
        df2 = load_csv(filename)
        os.remove(filename)
        # self.assertEqual(df1.equals(df2), True)
        self.assertEqual(df1.shape, df2.shape)


        
if __name__ == '__main__':
    unittest.main()

# ****************************************************************
# End of File
# ****************************************************************
