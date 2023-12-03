# *******************************************************************
# Data Manipulation Functions.
# *******************************************************************

# Data science imports
import numpy as np
import pandas as pd
import math
import logging
import itertools

from utils.utils import timing

# ----------------------------------------------------------

def flatten(data):
    'Flattens a list of nested lists.'
    return [item for sublist in data for item in sublist]


# ----------------------------------------------------------

def split_list(mylist, n=2):
    'Returns mylist partitioned into n_parts evenly distributed sublists.'

    # Compute offset indices
    parts = int(len(mylist)/n)
    indices = list(map(lambda x: [i*n+x for i in range(parts)], range(n)))
    
    # Insert list elements
    sublists = list(map(lambda l: [mylist[i] for i in l], indices))
    # Add trailing elements
    sublists = sublists + [mylist[n*parts:]]
    return sublists


# ----------------------------------------------------------

def split_dict(mydict, n=2):
    'Splits mydict into n pieces.'
    
    i = itertools.cycle(range(n))       
    split = [dict() for _ in range(n)]
    for k, v in mydict.items():
        split[next(i)][k] = v
        
    return split


# ----------------------------------------------------------

def chunk_dict(mydict, n=5):
    'Returns a subset of mydict of length n.'

    return {k: mydict[k] for k in list(refs.keys())[:n]}


# ----------------------------------------------------------

def split_dataframe(df, n=4):
    'Splits df into n+1 dataframes.'
    
    size = math.trunc(df.shape[0] / n)
    dfs = [df[i*size: ((i+1)*size)-1] for i in range(n-1)]
    dfs.append(df[(n-1)*size:])
    
    return dfs


# -------------------------------------------------------------------

def column_to_str(df, col):
    'Converts a non-string based column to string.'

    def stringify(text):
        if type(text) == str and len(text) > 0 and text[0] == '"':
            text = text[1:-1]
        elif type(text) == int:
            text = f'{text}'
        elif type(text) == float and math.isnan(float(text)):
            text = 'unknown'
        elif type(text) == float:
            text = f'{int(text)}'
        else:
            text = str(text)
        return text
    
    df[col] = df[col].apply(stringify)
    return df


# --------------------------------------------------------------

def filter_df(df, col, values):
    'Returns a dataframe that only has values of a column'
    rows = []
    for index, row in df.iterrows():
        if row[col] in values:
            rows.append(row)
    return pd.DataFrame(rows, columns=df.columns)


# --------------------------------------------------------------

def df_to_list(df):
    columns = list(df.columns)
    rows = np.ndarray.tolist(df.values)
    return columns, rows


# ----------------------------------------------------------

def shuffle_data(df):
    'Shuffles data using Pandas sample function.'
    df = df.sample(frac=1).reset_index(drop=True)
    return df


# -------------------------------------------------------------------

def partition_data(data, training_percent):
    'Partitons array data into training and test by training_percent'
    rows = data.shape[0]
    training_size = int(round(rows * training_percent / 100, 2))
    training_data = data[:training_size]
    test_data = data[training_size:]
    return training_data, test_data


# -------------------------------------------------------------------

# TODO: Investigate why this SKLearn fn generates an error with
# LMFR training data.

# def partition_data(data, acol='class', training_percent=0.8):
#     'Partitons array data into training and test by training_percent'
#     X, y = data, data[acol]
#     partitions = train_test_split(X, y, train_size=training_percent)
#     training_data, test_data, _, _ = partitions
#     return training_data, test_data


# ----------------------------------------------------------

def value_counts_dict(df, column):
    'Returns the value counts of column as a dictionary.'
    s = df[column].value_counts()
    return dict(zip(s.index, s.values))


# --------------------------------------------------------------

@timing
def text_length_stats(df, tcol='text'):
    'Returns the min, max and average number of characters and tokens.'
    lengths, counts = [], []
    for index, row in df.iterrows():
        tokens = tokenize_designation(row[tcol])
        counts.append(len(tokens))
        lengths.append(len(' '.jioin(tokens)))
    average_length = round(sum(lengths) / df.shape[0], 2)
    average_count = round(sum(counts) / df.shape[0], 2)
    min_length, max_length = min(lengths), max(lengths)
    min_count, max_count = min(counts), max(counts)
    return [min_length, max_length, average_length], \
           [min_count, max_count, average_count]


# --------------------------------------------------------------
# Printing and Logging
# --------------------------------------------------------------

def loginit(filename):
    pathname = make_logging_pathname(filename)
    logging.basicConfig(filename=pathname, level=logging.INFO)


def logmessage(message):
    logging.info(message)


def logprint(message):
    print(message)
    logging.info(message)


# ---------------------------------------------------------------
# Clean Data
# ---------------------------------------------------------------

def clean_data(df, tcol='text', ccol='class', tokens_col='tokens',
               min_tokens=3):
    'Eliminates duplicates and sentences with less than min_tokens.'

    print('\nCleaning training data...')

    df = df.copy()

    # Remove rows whose text is too short
    if tokens_col in df.columns:
        df = df.loc[df[tokens_col].map(len) >= min_tokens]

    # Replace NaNs
    df = df.fillna('')

    # Return clean df
    return df


# ---------------------------------------------------------------
# Tabular Data
# ---------------------------------------------------------------

def list_to_table(data, cols):
    data = np.array(data)
    arrays = np.array_split(data, cols)
    results = list(map(lambda x: list(x), arrays))
    return results


def normalize_table(table):
    max_len = max(map(lambda x: len(x), table))
    for row in table:
        if len(row) < max_len:
            for i in range(max_len - len(row)):
                row.append('')
    return table


# --------------------------------------------------------------
# Saving Composite Data Structures
# --------------------------------------------------------------

def save_table(table, filename):
    table = normalize_table(table)
    df = pd.DataFrame(table)
    write_excel(df, filename)


# --------------------------------------------------------------

def save_dict(data, filename, pprint=True):
    indent = 4 if pprint is True else 0
    with open(filename, 'w', encoding='utf-8') as data_file:
        json.dump(data, data_file, ensure_ascii=False, indent=indent)
    return True


# --------------------------------------------------------------
# Split Dataframe
# --------------------------------------------------------------

def split_dataframe(df, n):
    size = math.trunc(df.shape[0] / n)
    print(f'Size: {size}')
    dfs = [df.loc[i*size: ((i+1)*size)-1] for i in range(n-1)]
    dfs.append(df.loc[(n-1)*size:])
    return dfs


def count_class_instances(df, ccol='class'):
    'Count total number of instances of each class'
    counts = df[ccol].value_counts()
    class_counts = dict(zip(counts.index, counts.values))
    return class_counts


# ****************************************************************
# End of File
# ****************************************************************

