# ****************************************************************
# Utility Functions
# ****************************************************************
#
# NB: The functions in this module are potentially used by other
# repositories so refrain from introducing breaking changes without
# first checking the consumers of this module.

# This module contains utility functions for manipulating files, directories,
# web related functionality and data cleaning and processing functions that
# are used throughout the system. The code in this file is divided into the
# following categories:
#
# Part 1. Local File System
# Part 2: Google Cloud Storage
# Part 3. Natural Language Processing
# Part 4: Data Manipulation
#
# ****************************************************************

# Standard Python imports
import os
import re
import csv
import json

from os import listdir
from os.path import isfile, join

import pandas as pd
from time import time


# ----------------------------------------------------------------------
# Generic File Operations
# ----------------------------------------------------------------------

def files_in_dir(dir, filter=''):
    files = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]
    return [f for f in files if filter in f]


# ----------------------------------------------------------------------

def file_exists(filename):
    return os.path.exists(filename)

# ----------------------------------------------------------------------

def delete_file(pathname):
    os.remove(file)
    return True

# ----------------------------------------------------------------------
# Load and save text files as strings
# ----------------------------------------------------------------------

def load_text_file(pathname):
    fileptr = open(pathname, 'r')
    html = fileptr.read()
    fileptr.close()
    return html


# ----------------------------------------------------------------------

def save_text_file(html, pathname):
    fileptr = open(pathname, 'w')
    fileptr.write(html)
    fileptr.close()
    return True


# ------------------------------------------------------------
# Load & Save CSV
# ------------------------------------------------------------

def load_csv(filename, delimiter=',', index_col=False):
    'Loads the specified CSV file using the specified parameters.'

    df = pd.read_csv(filename, delimiter=delimiter,
                     index_col=index_col)
    return df


def save_csv(df, filename, index=False):

    'Saves the specified CSV file using the specified parameters.'
    df.to_csv(filename, index=index)
    return True


# ------------------------------------------------------------------------------
# Append CSV Rows
# ------------------------------------------------------------------------------

def write_csv_header(header, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
    return True


# ------------------------------------------------------------------------------

def append_csv_rows(rows, filename):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        for row in rows:
            writer.writerow(row)
    return True


# --------------------------------------------------------------
# Excel Files
# --------------------------------------------------------------

def read_excel(file):
    return pd.read_excel(file, engine='openpyxl')


# --------------------------------------------------------------

def write_excel(df, file, index=False):
    df.to_excel(file, engine='openpyxl', index=index)
    return True


# --------------------------------------------------------------
# Json Files
# --------------------------------------------------------------

def load_dict(filename):
    with open(filename, 'r') as data_file:
        data = json.load(data_file)
    return data


# --------------------------------------------------------------

def save_dict(data, filename, pprint=True):
    indent = 4 if pprint is True else 0
    with open(filename, 'w', encoding='utf-8') as data_file:
        json.dump(data, data_file, ensure_ascii=False, indent=indent)
    return True


# --------------------------------------------------------------
# Pickle Files
# --------------------------------------------------------------

def open_pickle_data(path):
    return open(path, 'rb')


# ****************************************************************
# End of File
# ****************************************************************
