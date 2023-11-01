# *********************************************************************
# Miscellaneous Utilities
# *********************************************************************

# Standard Python imports
from time import time
from functools import wraps


# --------------------------------------------------------------
# Basic Timing Decorator
# --------------------------------------------------------------

def timing(func):
    @wraps(func)
    def wrap(*args, **kw):
        start = time()
        result = func(*args, **kw)
        end = time()
        elapsed = round((end-start) / 60, 2)
        print(f'{func.__name__} took {elapsed} minutes.')
        return result
    return wrap


# ****************************************************************
# End of File
# ****************************************************************

