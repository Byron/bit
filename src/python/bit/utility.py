#-*-coding:utf-8-*-
"""
@package bit.utility
@brief Misc utilities for use by everyone in the IT department

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['date_string_to_datetime', 'ratio_to_float', 'bool_label_to_bool',
           'delta_to_tty_string', 'float_percent_to_tty_string', 'datetime_to_date_string', 'datetime_days_ago',
           'seconds_to_datetime', 'delta_to_seconds', 'Table', 'ravg', 'rsum', 'float_to_tty_string', 'graphite_submit',
           'DistinctStringReducer', 'TerminatableThread', 'IDParser',
           'ExpiringCache', 'CachingIDParser', 'ThreadsafeCachingIDParser', 'datetime_to_date_time_string',
           'StringMapper', 'utc_datetime_to_date_time_string', 'none_support']

from time import (strptime,
                  gmtime,
                  time,
                  timezone )
from datetime import (timedelta,
                      datetime,
                      date )

import os
import sys

from butility.compat import pickle
from struct import pack
import socket
import Queue
import inspect
import subprocess
import threading

from butility import (Thread,
                        DictObject)

data_unit_multipliers = {
                'k' : 1024,
                'm' : 1024**2,
                'g' : 1024**3,
                't' : 1024**4,
                'p' : 1024**5,
                '%' : 1,
}

time_unit_multipliers = {
        's' : 1,
        'h' : 60**2,
        'd' : 60**2 * 24,
        'w' : 60**2 * 24 * 7,
        'm' : 60**2 * 24 * 30,
        'y' : 60**2 * 24 * 365
    }

# -------------------------
## @name Constants
# @{

## Default carbon port (for graphite)
CARBON_PORT = 2004
epoch = datetime(1970,1,1)
date_timedelta = timedelta(0, timezone)

## -- End Constants -- @}


# ==============================================================================
## @name Reducers
# ------------------------------------------------------------------------------
# For use by the Table type
## @{

def ravg(prev, cur):
    """@return average of both values"""
    return (prev + cur) / 2

def rsum(prev, cur):
    """The running total"""
    return prev + cur

class DistinctStringReducer(object):
    """A reducer to count occurrences of distinct strings that are fed to him"""
    __slots__ = ('string_set')

    def __init__(self):
        self.string_set = set()

    def __call__(self, prev, val):
        self.string_set.add(prev)
        self.string_set.add(val)
        return val

    def __str__(self):
        return '#%i' % len(self.string_set)

# end class StringCountReducer

## -- End Reducers -- @}


# ==============================================================================
## @name Utilities
# ------------------------------------------------------------------------------
## @{

def none_support(string_converter):
    """Decorator to allow string converters to support None"""
    def wrapper(value):
        if value is None:
            return '-'
        return string_converter(value)
    # end wrapper
    wrapper.__name__ = string_converter.__name__
    return wrapper
    

## -- End Utilities -- @}




# ==============================================================================
## @name Utility Types
# ------------------------------------------------------------------------------
## @{

class Table(object):
    """A simple Table which consists of a schema and records"""
    __slots__ = (
                    ## A list of columns we have, as tuple of (name, type, converter[, reduce]) triplets
                    # The type is a function or type that can default-construct new values, or convert into its type
                    # from an input value.
                    # The converter is a function converting a record's value to any target type, usually a string
                    # The optional reduce function takes its previous result and the next value to produce
                    # a single result value which can be converted to the final desired type.
                    # reduce(prev, next) -> res
                    'columns',

                    ## A list of records, being a list of lists
                    'records'
                )

    def __init__(self, columns=None, records=None):
        """Initialize this instance"""
        self.columns = columns or list()
        self.records = records or list()

    # -------------------------
    ## @name Interface
    # @{

    def aggregate_record(self, predicate = lambda r: True):
        """Create a record which is the aggregate of all records for which predicate(r) returned True
        By default, it will average every number and date, ideally you provide your own reducer as third 
        entry in your column's description
        @param predicate a function to return True for each record that should take part in the aggregation.
        By default, all values take part
        @return the aggregate record as matching our schema - you can post-process it and append it to your records"""
        # map all reducers, determine non-string indices
        reduced = list()
        reducers = list()
        reduce_ids = list()

        for cid, info in enumerate(self.columns):
            if len(info) == 3:
                name, default, conv = info
                reducer = None
                if isinstance(default(), (int, float, timedelta)):
                    reducer = ravg
                # end handle default reducer
            else:
                assert len(info) == 4
                name, default, conv, reducer = info
            # end handle

            reducers.append(reducer)
            reduced.append(None)

            if reducer is not None:
                reduce_ids.append(cid)
            # end setup default value
        # end for each info

        for rec in self.records:
            if not predicate(rec):
                continue
            # end apply predicate

            for rid in reduce_ids:
                pval = reduced[rid]
                val = rec[rid]
                if pval is None:
                    # init reduced value
                    reduced[rid] = val
                elif val is not None:
                    reduced[rid] = reducers[rid](pval, val)
                # end handle pval
            # end for each id we should handle
        # end for each record

        # We know the special needs of our StringReducer
        for rid in xrange(len(reduced)):
            if isinstance(reducers[rid], DistinctStringReducer):
                reduced[rid] = str(reducers[rid])
            # end resolve DistinctStringReducer
        # end for each reduced value

        return reduced
    

    def is_empty(self):
        """@return True if the table has no content"""
        return not self.records
        
    ## -- End Interface -- @}

# end class Table


class TerminatableThread(Thread):
    """A simple thread able to terminate itself on behalf of the user.
    
    Terminate a thread as follows:
    
    t.stop_and_join()
    
    Derived classes call _should_terminate() to determine whether they should 
    abort gracefully
    """
    __slots__ = '_terminate'
    
    def __init__(self, *args, **kwargs):
        super(TerminatableThread, self).__init__(*args, **kwargs)
        self._terminate = False
        

    # -------------------------
    ## @name Subclass Interface
    # @{
    
    def _should_terminate(self):
        """:return: True if this thread should terminate its operation immediately"""
        return self._terminate
        
    ## -- End Subclass Interface -- @}
        
    # -------------------------
    ## @name Interface
    # @{
    
    def cancel(self):
        """Schedule this thread to be terminated as soon as possible.
        @note this method does not block."""
        self._terminate = True
    
    def stop_and_join(self):
        """Ask the thread to stop its operation and wait for it to terminate
        :note: Depending on the implenetation, this might block a moment"""
        self.cancel()
        self.join()

    ## -- End Interface -- @}

# end class TerminatableThread


class WorkerThread(TerminatableThread):
    """
    This base allows to call functions on class instances natively and retrieve
    their results asynchronously using a queue.
    The thread runs forever unless it receives the terminate signal using 
    its task queue.
    
    Tasks could be anything, but should usually be class methods and arguments to
    allow the following:
    
    inq = Queue()
    outq = Queue()
    w = WorkerThread(inq, outq)
    w.start()
    inq.put((WorkerThread.<method>, args, kwargs))
    res = outq.get()
    
    finally we call quit to terminate asap.
    
    alternatively, you can make a call more intuitively - the output is the output queue
    allowing you to get the result right away or later
    w.call(arg, kwarg='value').get()
    
    inq.put(WorkerThread.quit)
    w.join()
    
    You may provide the following tuples as task:
    t[0] = class method, function or instance method
    t[1] = optional, tuple or list of arguments to pass to the routine
    t[2] = optional, dictionary of keyword arguments to pass to the routine
    """
    __slots__ = ('log', 'inq', 'outq')
    
    class InvalidRoutineError(Exception):
        """Class sent as return value in case of an error"""

    class QuitException(Exception):
        """Raised to signal we should quit and stop processing tasks"""
    
    # end class QuitExceptionx
        
    def __init__(self, log, inq = None, outq = None):
        super(WorkerThread, self).__init__()
        self.inq = inq or Queue.Queue()
        self.outq = outq or Queue.Queue()
        self.log = log
    
    def call(self, function, *args, **kwargs):
        """Method that makes the call to the worker using the input queue, 
        @return self
        
        @param function can be a standalone function unrelated to this class, 
            a class method of this class or any instance method.
            If it is a string, it will be considered a function residing on this instance
        @param *args arguments to pass to function
        @param **kwargs kwargs to pass to function"""
        self.inq.put((function, args, kwargs))
        return self
    
    def stop_and_join(self):
        """Send the stop signal to terminate, then join"""
        self._terminate = True
        self.inq.put(self.quit)
        self.join()
        self._terminated()

    def cancel(self):
        super(WorkerThread, self).cancel()
        self.inq.put(self.quit)
    
    def run(self):
        """Process input tasks until we receive the quit signal"""
        while True:
            if self._should_terminate():
                break
            # END check for stop request
            routine = self.quit
            args = tuple()
            kwargs = dict()
            tasktuple = self.inq.get()
            
            if isinstance(tasktuple, (tuple, list)):
                if len(tasktuple) == 3:
                    routine, args, kwargs = tasktuple
                elif len(tasktuple) == 2:
                    routine, args = tasktuple
                elif len(tasktuple) == 1:
                    routine = tasktuple[0]
                # END tasktuple length check
            elif inspect.isroutine(tasktuple):
                routine = tasktuple
            # END tasktuple handling
            
            try:
                rval = None
                if inspect.ismethod(routine):
                    if routine.im_self is None:
                        rval = routine(self, *args, **kwargs)
                    else:
                        rval = routine(*args, **kwargs)
                elif inspect.isroutine(routine):
                    rval = routine(*args, **kwargs)
                else:
                    # ignore unknown items
                    self.log.error("%s: task %s was not understood - terminating", self.name, str(tasktuple))
                    self.outq.put(self.InvalidRoutineError(routine))
                    break
                # END make routine call
                self.outq.put(rval)
            except self.QuitException:
                break
            except Exception,e:
                self.log.error("%s: Task %s raised unhandled exception: %s", self.name, str(tasktuple), str(e), exc_info=True)
                self.outq.put(e)
            # END routine exception handling
        # END endless loop
    
    def quit(self):
        raise self.QuitException()
    
#} END classes

## -- End Utility Types -- @}


# ==============================================================================
## @name Utilities
# ------------------------------------------------------------------------------
## @{


def datetime_to_date_string(datetime):
    """@return a string representation of the given datetime object, just providing the date"""
    return str(date(datetime.year, datetime.month, datetime.day))

def datetime_to_date_time_string(datetime):
    """@return a string representation of the given datetime object, providing date and time"""
    return "%04i-%02i-%02i@%02i:%02ih" % (datetime.year, datetime.month, datetime.day,
                                         datetime.hour, datetime.minute)

def utc_datetime_to_date_time_string(datetime):
    """@return a representation of a datetime object in UTC, converted to the local timezone"""
    return datetime_to_date_time_string(datetime - date_timedelta)

def datetime_to_seconds(datetime):
    """@return given datetime object as seconds since epoch"""
    d = datetime - epoch
    return (d.microseconds + (d.seconds + d.days * 24 * 3600) * 10**6) / 10**6
    
def date_string_to_datetime(date_string):
    """@return a datetime object matching the given date_string
    @param date_string formatted like Thu May 12  4:21 2011"""
    return datetime(*strptime(date_string, '%a %b %d  %H:%M %Y')[:6])

def ratio_to_float(ratio_string):
    """@return a string like 1.24x as float"""
    return float(ratio_string[:-1])

def bool_label_to_bool(label):
    """@return A label like yes/no as Bool instance"""
    return label in ('yes', 'on', 'active', 'enabled')
    
def delta_to_tty_string(delta):
    """Convert a timedelta object (datetime - datetime) into a human readable string like '5m ago', 
    or 'in 5y'
    @param delta deltatime instance
    @note currently only deals with the past"""
    prefix = ''
    suffix = ' ago'
    if delta.seconds < 0 or delta.days < 0:
        prefix = 'in '
        suffix = ''
    # end handle the future
    assert delta.seconds >= 0 and delta.days >= 0, "cannot currently handle negative deltas"
    res = ''
    years = days = 0
    if delta.days:
        years = delta.days / 365
        days = (delta.days - years * 365) % 365
        if years:
            res += '%iy' % years
        res += '%id' % (days)
    # if we are in years scale, drop hours and minutes
    if not years and delta.seconds:
        hours = delta.seconds / (60**2)
        minutes = (delta.seconds - hours * (60**2)) / 60
        if hours:
            res += '%ih' % hours
        # use minutes in any way, or we have nothing !
        if days < 1:
            res += '%im'% minutes
    # end handle seconds
    if delta.days or delta.seconds:
        res += suffix
        res = prefix + res
    else:
        res = 'just now'
    # end handle special case of time not being in the past
    return res

def float_percent_to_tty_string(percent):
    """@return a string indicating a percentage"""
    return '%.0f%%' % percent

def float_to_tty_string(value):
    """@return a string indicating a float value with acceptable preceision"""
    return '%.2f' % value

def seconds_to_datetime(seconds_since_epoch):
    """@return datetime object equivalent to the given seconds_since_epoch"""
    return datetime(*gmtime(seconds_since_epoch)[:6])

def datetime_days_ago(seconds_since_epoch, days):
    """@return a datetime object that is `days` before the time described by seconds_since_epoch"""
    return seconds_to_datetime(seconds_since_epoch - (days * 60*60*24))

def delta_to_seconds(delta):
    """Convert a time-delta to seconds"""
    return delta.days * (3600*24) + delta.seconds

def graphite_submit(carbon_host, sample_list, port=CARBON_PORT):
    """Send the given sample_list to the given carbon_host
    @param carbon_host sufficiently qualified host name or ip quatruple as string
    @param port to connect to, with a suitable default
    @param sample_list a list in the following format: [(path, (unix_timestamp, numeric))]"""
    # make sure payload doesn't get too big - therefore we will just chunk it up into 1000 items, allowing
    # each sample to be 1000 bytes
    max_size = (2**20) - 100
    cs = 1000
    for cursor in xrange(0, len(sample_list), cs):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((socket.gethostbyname(carbon_host), port))
        try:
            payload = pickle.dumps(sample_list[cursor:cursor+cs])
            message = pack('!L', len(payload)) + payload
            sock.sendall(message)
        finally:
            sock.close()
        # end assure socket is closed
    # end for each chunk


class ExpiringCache(object):
    """Simple implementation of a caching dictionary where each entry has a lifetime.
    Once the object expired, the cached value will be discarded.

    None has a special meaning, as is returned if the cache is expired. Even though you can set it, 
    when getting the value you wouldn't know if the cache is expired or if it is your value.
    """
    __slots__ = ('_store')

    def __init__(self):
        """Initialize this instance"""
        self._store = dict()
        
    def __len__(self):
        return len(self._store)

    def __contains__(self, key):
        return key in self._store

    # -------------------------
    ## @name Interface
    # @{

    def get(self, key):
        """@return value at Key (may be None as well), or None, if the value is expired or if it didn't exist
        @note get mutates the internal storage as it may drop values upon refresh, or update them if update_fun was set."""
        if key not in self._store:
            return None
        # end ignore missing entries

        value, it, ttl, update = self._store[key]
        ct = time()
        if ct > it + ttl:
            if update:
                value = update(key, value)
                self._store[key] = (value, ct, ttl, update)
            # end update existing value

            if update is None or value is None:
                del self._store[key]
                return None
            # end
        # end handle expired cache

        return value

    def set(self, key, value, time_to_live=sys.float_info.max, update_fun=None):
        """Set the given value to be found at the given key, as long as the lifetime of it is positive, it will 
        be returned by successive calls of get(key)
        @param key at which to store the value
        @param value any object, including None. Note that None as initial value will NOT trigger the update fun on get() call
        @param time_to_live time in seconds (int or float) after which the object should expire and be expunged from the cache
        @param update_fun if not None, f(key, expired_value) -> new_value . If set, a function that returns the new value 
        given the now expired one. If it returns None, the value is expired
        @return this instance"""
        self._store[key] = (value, time(), time_to_live, update_fun)
        return self
        
    ## -- End Interface -- @}

# end class ExpiringCache


class IDParser(object):
    """A simple utility to parse the result of the linux 'id' call and make it usable in code.

    To make things much easier, the interface is trimmed down to operate on individual users only
    """
    __slots__ = ()

    # -------------------------
    ## @name Configuration
    # @{

    ## Full path to the id program that we are to use
    ID_PATH = '/usr/bin/id'

    ## All keys which are always have multiple values
    multi_value_keys = ('groups')
    
    ## -- End Configuration -- @}

    # -------------------------
    ## @name Utilities
    # @{

    def _id_path(self):
        """@return path to the id program to use"""
        return self.ID_PATH

    def _call_id(self, name):
        """@return stdout of the id call with the given user name, or None if id returned with an error
        The latter happens if the user doesn't exist
        """
        assert os.name == 'posix'
        id_path = self._id_path()
        assert os.path.isfile(id_path), "ID program could not be found at '%s'" % id_path

        proc = subprocess.Popen((id_path, name), shell=False, stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            return None
        # end handle invalid name

        # Not critical, might want to remove this ... 
        assert not stderr, "There should be no stderr when return code is 0"
        return stdout.strip()


    ## -- End Utilities -- @}

    # -------------------------
    ## @name Parser Utilities
    # @{

    def _parse_single_value(self, value):
        """@return tuple of int, string|None"""
        integer, string = None, None
        tokens = value.split('(')   # (...) is optional
        integer = int(tokens[0])

        if len(tokens) == 2:
            string = tokens[1][:-1]    # truncate )
            if not string:
                string = None
            # end invalidate empty strings
        # end handle string parsing

        return integer, string

    def _parse_token(self, token):
        """@return final parsed key, value pair. Value can be single tuple, or list of tuples"""
        assert '=' in token, "need key=value pair"
        key, value = token.split('=')

        if ',' in value or key in self.multi_value_keys:
            values = list()
            for item in value.split(','):
                values.append(self._parse_single_value(item))
            # end for each value
            return key, values
        else:
            return key, self._parse_single_value(value)
        # end handle value, multi-value
    
    ## -- End Parser Utilities -- @}

    # -------------------------
    ## @name Interface
    # @{

    def parse(self, login):
        """Parse all id-related information of the given login name and return it
        @return None if the login is invalid or if there is no id information for some reason. Otherwise, 
        return a dict-object with the following fields:
        * uid = (int, login)
        * gid = (int, name)
        * groups = [(int, name|None), ...]
        """
        tokens = self._call_id(login)
        if tokens is None:
            return None
        # end ignore failed calls

        res = dict()
        tokens = tokens.split(' ')

        for token in tokens:
            key, value = self._parse_token(token)
            res[key] = value
        # end for each token

        return DictObject(res)
    
    ## -- End Interface -- @}
# end class IDParser


class CachingIDParser(IDParser):
    """An ID parser which caches parse results for a given amount of seconds"""
    __slots__ = ('_cache', '_ttl')

    def __init__(self, time_to_live):
        """Initialize this instance
        @param time_to_live amount of time in seconds our id information should remain valid for"""
        self._ttl = time_to_live
        self._cache = ExpiringCache()

    
    # -------------------------
    ## @name Interface Overrides
    # @{

    def parse(self, login):
        """Similar to subclass, but implements caching"""
        if login not in self._cache:
            # First time call - set up the cache
            update = lambda key, pv: super(CachingIDParser, self).parse(key)
            value = update(login, None)
            self._cache.set(login, value, self._ttl, update)
            return value
        # end handle initial setup (we expect auto-update)

        return self._cache.get(login)

    def set_cache_expires_after(self, time_to_live):
        """Set how fast items in this cache are expiring.
        If set, the entire cache will be expired right away
        @return this instance"""
        self._cache = ExpiringCache()
        self._ttl = time_to_live
        return self

    ## -- End Interface Overrides -- @}    

# end class CachingIDParser


class ThreadsafeCachingIDParser(CachingIDParser):
    """An implementation that will deal with multiple threads gracefully. This is especially important
    when subprocesses are called, which could be invoked multiple times at once.
    Even though the result will be correct, it's kind of nonsense"""
    __slots__ = ()

    # Yes, we allow only one id call per process
    _lock = threading.Lock()

    def parse(self, login):
        """As in CachingIDParser, but threadsafe"""
        self._lock.acquire()

        try:
            return super(ThreadsafeCachingIDParser, self).parse(login)
        finally:
            self._lock.release()
        # end assure lock is released

# end class ThreadsafeCachingIDParser


class StringMapper(object):
    """Allows to map a path from a source to a destination based on a simple map"""
    __slots__ = ('_map_list')


    def __init__(self, map_list=list()):
        """
        @param map_list possibly unordered list of source-destination pairs, 
        where source is at 2N, and destination is at 2N+1"""
        assert len(map_list) % 2 == 0, "Need to specify string map as pairs: source and destination"
        self._map_list = sorted(zip(map_list[0::2], map_list[1::2]), # zip the list together, to get a tuple
                                    key=lambda t: len(t[0]),         # sort by first element string length
                                    reverse=True)                    # longest string first        


    # -------------------------
    ## @name Interface
    # @{

    def apply(self, string, reverse=False):
        """Maps strings looked up in a string map.
        @param string the string to map
        @param reverse if False, map from source to destination. Otherwise, map form destination to source.
        Used for reverse mapping the string again.
        @returns the mapped string, otherwise the unmodified input string
        """
        assert string

        idxKey      = (0 if reverse == False else 1)
        idxValue    = (1 if reverse == False else 0)

        for entry in self._map_list:
            if string.startswith(entry[idxKey]):
                return string.replace(string[:len(entry[idxKey])], entry[idxValue])
            # end match found
        #end loop through string map

        return string

    ## -- End Interface -- @}
# end class StringMapper

    
## -- End Utilities -- @}
    


