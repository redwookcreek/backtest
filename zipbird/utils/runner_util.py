import time
from functools import wraps
import warnings

def supress_warnings():
    warnings.simplefilter(action='ignore', category=FutureWarning, append=True)
    # supress empyrical warnings "invalid value encountered in divide"
    warnings.simplefilter(action='ignore', category=RuntimeWarning, lineno=710, append=True)
    warnings.simplefilter(action='ignore', category=RuntimeWarning, lineno=799, append=True)

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        start = time.perf_counter()
        result = f(*args, **kw)
        seconds = time.perf_counter() - start
        minutes = seconds / 60
        print('func:%r took: %.2f minutes' % \
          (f.__name__, minutes))
        return result
    return wrap