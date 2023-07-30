# ****************************************************************
# KGML NLP Functions
# ****************************************************************
#
# NB: The functions in this module are potentially used by other
# repositories so refrain from introducing breaking changes without
# first checking the consumers of this module.

# This module contains utility functions for NLP related tasks
#
# ****************************************************************

# Standard Python imports
import os
import re
import logging
from functools import reduce

# Data science imports
from numpy import dtype
import pandas as pd
import pandas_gbq
import nltk


# ------------------------------------------------------------
# Count Words
# ------------------------------------------------------------

def count_words(sentences, tokens=None, min_count=1000):
    'Return a datframe of word counts greater than min_count'

    words = {}

    # Count words across all sentences
    for sentence in sentences:
        tokens = tokenize_designation(sentence)
        for token in tokens:
            words.update({token: words.get(token, 0) + 1})

    # Present the results in a dataframe
    if len(words) > 0:
        df = pd.DataFrame.from_dict(words, orient='index')
        df.columns = ['count']
        words = list(zip(list(df.index), list(df['count'])))
        df = pd.DataFrame(words, columns=['word', 'count'])
        df = df.loc[df['count'] >= min_count]
        df = df.sort_values(['count'], ascending=False)
        return df
    else:
        return None


# -----------------------------------------------------------------
# Tokens Distributions
# ------------------------------------------------------------------

# NB: These functions are useful for determing a reasonable value
# a fixed sequence length as well as understanding the nature of the
# text corpus.

def tokens_distributions(df, tcol='text'):
    'Returns the distribution of the number of tokens per text column.'
    
    size, dist, total = df.shape[0], {},  0
    min_len, max_len = 1000000, 0

    for index, row in df.iterrows():
        tokens = tokenize_designation(row[tcol])
        count = len(tokens)
        dist.update({count: dist.get(count, 0) + 1})
        if count < min_len:
            min_len = count
        if count > max_len:
            max_len = count
        total += count

    # Rerturn distribution and the avg, min and max lengths.
    return dist, round(total/size), min_len, max_len


# ------------------------------------------------------------------

def text_ranges(df, tcol='text'):
    'Partitions text counts by powers of 10 based on token count.'
    
    text_lengths = {}
    for index, row in df.iterrows():
        tokens = tokenize_designation(row[tcol])
        count = len(tokens)
        text_lengths.update({count: text_lengths.get(count, 0) + 1})
    brackets = [10**(i+1) for i in range(4)]
    results = {}
    for k in text_lengths.keys():
        for b in brackets:
            if k < b:
                results.update({b: results.get(b, 0) + text_lengths[k]})
                break
            
    return results


# ---------------------------------------------------------------
# Find First Word
# ---------------------------------------------------------------

def find_first_word(words, descriptions):
    'Return the word in words that occurs earliest on average in descriptions.'
    
    position_results = []
    for word in words:
        positions = list(map(lambda desc: desc.find(word), descriptions))
        position = int(reduce(lambda a, b: a + b, positions) / len(positions))
        position_results.append([word, position])
        position_results.sort(key=lambda x: x[1], reverse=True)
        
    return position_results[0][0]


# --------------------------------------------------------------
# Select Best Word
# --------------------------------------------------------------

# Return the most frequently ocuring word in descriptions. Ties
# are handled using find_first_word above.

# TODO: Handle sequences of words.

def select_best_word(descriptions, words_df):
    'Return the most frequently ocuring word in descriptions.'
    
    max_count = words_df['count'].max()
    max_words = list(words_df.loc[words_df['count'] == max_count]['word'])
    
    if len(max_words) == 1:
        return max_words[0]
    else:
        return find_first_word(max_words, descriptions)


# -----------------------------------------------------------------
# Aphabetic words
# -----------------------------------------------------------------

def alphabetic_word_p(word):
    'Returns True if word contains only alpabetic utf characters.'

    alphap = re.match(r'[^\W\d]*$', word)
    
    return True if alphap is not None else False


# ----------------------------------------------------------------
# Tokenize Text
# ----------------------------------------------------------------

# Currently not using stop words because requires manual download of
# NLTK language models. Need to contextualize for BU's

STOP_WORDS = []
# STOP_WORDS = stopwords.words('french')

PUNCTUATIONS = [',', '.', ';', ':', '(', ')', '[', ']', '<', '>',
                '!', '@', '#', '$', '%', '^', '&', '*', '\\']


# ----------------------------------------------------------------

# TODO: Use a tokenizer other than the one from NLTK.

def tokenize_text(text, alpha_only=False, stopwords=None):
    'Tokenize text with NLTK and apply filters.'

    # Tokenize using NLTK tokenizer
    text = str(text) if type(text) != str else text
    tokens = nltk.word_tokenize(text.lower())

    if type(tokens) == list:
        # Apply filters to tokens
        filtered_tokens = []
        for token in tokens:
            if token not in PUNCTUATIONS:
                if stopwords is None:
                    if alpha_only is False:
                        filtered_tokens.append(token)
                    elif alphabetic_word_p(token):
                        filtered_tokens.append(token)
                    elif token not in STOP_WORDS:
                        if alpha_only is False:
                            filtered_tokens.append(token)
                        elif alphabetic_word_p(token):
                            filtered_tokens.append(token)
    else:
        filtered_tokens = []

    return filtered_tokens


# --------------------------------------------------------------------

def tokenize_designation(text):
    'Return a list of tokens with alphanumeric and stopwords retained.'
    
    # return [x for x in text.split(' ') if x not in PUNCTUATIONS]
    return tokenize_text(text, alpha_only=False, stopwords=None)


# -------------------------------------------------------------------
# Clean designations
# -------------------------------------------------------------------

def clean_designations(df, tcol='designation'):
    def clean(text):
        if type(text) == str:
            text = text.replace("'", "")
            text = text.replace(",", "")
            text = text.replace('"', '')
            return text
        else:
            return text
    df[tcol] = df[tcol].apply(clean)
    return df

                
# -------------------------------------------------------------------
# Scan Sentences
# -------------------------------------------------------------------

# Need to find a better sentence parser from TF or other library

def scan_sentences(text):
    'Splits text into sentences based on periods.'
    
    return text.split('.')


# ******************************************************************
# Part 3: Data Augmentation
# ******************************************************************

# ------------------------------------------------------------------
# PARTITION
# ------------------------------------------------------------------

def partition(tokens, partitions=2):
    size = int(np.round(len(tokens)/partitions))
    token_sets = [tokens[i*size:(i+1)*size] for i in list(range(partitions))]
    return token_sets


# ------------------------------------------------------------------

def interleave_lists(l1, l2):
    c = list(chain.from_iterable(zip_longest(l1, l2)))
    return [x for x in c if x is not None]


# ------------------------------------------------------------------

# This combines the tokens in l1 with the tokens in l2 in one of three possible ways:
# (1) halving, (2) interleaving and (3) reversing.


def apply_combination_method(l1, l2, method):
    'Combines tokens in l1 and l2 in one of three possible ways.'
    
    if method == 'reversing':
        token_list = (l1 + l2)[::-1]
    elif method == 'interleaving':
        token_list = interleave_lists(l1, l2)
    else:
        token_list = l1 + l2
    return token_list

# ---------------------------------------------------------------------

# This considers all pairs of token lists from the two specified token lists
# and combines each pair in one of three possible ways (halving, interleaving
# and reversing) in order to augment the data.

def combine_token_lists(token_lists1, token_lists2, method='halving',
                        limit=None):
    'Returns an augmented set of token lists with n^2 - n new lists.'
    
    new_token_lists = []
    count = 0

    for l1 in token_lists2:
        for l2 in token_lists1:
            if (limit is None or count < limit) and l1 != [] and l2 != []:
                count += 1
                new_tokens = apply_combination_method(l1, l2, method)
                p = ' '.join(new_tokens)
                new_token_lists.append(p)
            else:
                break

    return new_token_lists


# --------------------------------------------------------------------
# Invent Data 
# --------------------------------------------------------------------

# This generates new sentences from the sentences in DF. Three text invention
# strategies are supported halving, interleaving and reversing.


def invent_data(df, tcol='text', ccol='class', method='halving', limit=100000):
    'Invents a quadratic amount of new data.'

    classes = list(df[ccol].unique())
    print('Inventing data for these ' +
          str(len(classes)) + ' classes:\n')
    print(str(df[ccol].value_counts()) + "\n")

    dfs = []

    # Handle each class seperately
    for c in classes:
        cdf = df.loc[df[ccol] == c]
        sentences = list(cdf[tcol])
        if len(sentences) < limit:
            token_lists = [tokenize_designation(s) for s in sentences]
            tokens_partitions = [partition(tokens) for tokens in token_lists]

            tlists1 = [p[0] for p in tokens_partitions]
            tlists2 = [p[1] for p in tokens_partitions]

            new_texts = combine_token_lists(tlists1, tlists2, method, limit)
            new_texts = new_texts[:limit]

            print(f'Invented {len(new_texts)} for class: {c}')
            if new_texts != []:
                new_values = [[x, c] for x in new_texts]
                new_df = pd.DataFrame(new_values, columns=[tcol, ccol])
                dfs.append(new_df)

    if dfs != []:
        # Combine dataframes
        results_df = pd.concat(dfs, axis=0)
        results_df = results_df.drop_duplicates()
        return results_df
    else:
        # Return empty dataframe
        return pd.DataFrame([], columns=[tcol, ccol])


# ****************************************************************
# End of File
# ****************************************************************
