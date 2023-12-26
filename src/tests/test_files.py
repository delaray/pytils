# ****************************************************************
# Unit Tests for files.py
# ****************************************************************

import os
import unittest
import pandas as pd

# Project imports
from utils.files import load_csv, save_csv


# CLI: python -m unittest tests/test_files.py
# CLI: coverage run -m unittest
# CLI: coverage report

class TestFiles(unittest.TestCase):

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
