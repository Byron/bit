#-*-coding:utf-8-*-
"""
@package itool.fsstat
@brief A module to deal with filesystem statistics

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import sys
import os
import hashlib
import socket
from itertools import chain

from os import (readlink,
                lstat )
from stat import S_ISLNK as islink
from stat import S_ISDIR as isdir
from stat import S_ISREG as isreg
from datetime import datetime
from binascii import a2b_hex

from time import (time,
                  gmtime )

try:
    # see 
    # https://pypi.python.org/pypi/lz4
    from lz4 import dumps as lz4dumps
except ImportError:
    print "Coulnd't import lz4 - average compression ratio computation will be disabled"
    lz4dumps = None
# end ignore missing lz4 compressor

from bit.utility import seconds_to_datetime
import bapp
from bapp import ApplicationSettingsMixin
from bkvstore import KeyValueStoreSchema
from .base import IToolSubCommand
from . import fsstat_schema

from butility import (Path,
                      int_to_size_string)
import bcmd.argparse as argparse

from sqlalchemy import (create_engine,
                        MetaData,
                        Index,
                        select,
                        bindparam,
                        Binary)


# ==============================================================================
## @name Utilities
# ------------------------------------------------------------------------------
## @{

def mb(bytes):
    """@return float of bytes in megabytes"""
    return bytes / float(1024**2)
    
def sqlite_view_from_file(sql_file):
    """@return string with raw sql creating a view from the given file"""
    return ('CREATE VIEW "%s" AS ' % sql_file.namebase()) + open(sql_file).read()
    
def is_url(url):
    """@return True if this seems to be a url"""
    return '://' in url
    
def to_ascii(uni_string):
    """Convert the given unicode string to  ascii
    @note it's crazy, the encoding is absolutely not under control, and it's easy to break the programs neck
    because it fails ! This is all too compicated !!"""
    try:
        return uni_string.encode('utf-8')
    except UnicodeDecodeError:
        return str(uni_string)
    # end handle code
    

## -- End Utilities -- @}


class Streamer(object):
    """A utility which streams a file in chunks of a given size, and calls a handler which can be implemented
    by subclasses"""
    __slots__ = (
                    '_reader',  # function with file.read semantics
                    'elapsed',  # seconds taken to stream the file, as float
                    'bytes',      # bytes read as int
                )
    
    # -------------------------
    ## @name Configuration
    # @{
    
    ## Size we try to read per chunk
    chunk_size = 25 * 1024**2
    
    ## -- End Configuration -- @}
    
    def __init__(self):
        self._reader = None
        
    # -------------------------
    ## @name Subclass Interface
    # @{
    
    def _handle_chunk(self, chunk):
        """Perform an operation on the chunk. Nothing by default
        @param chunk a chunk which is garantueed not to be empty"""
    
    def _stream_begin(self):
        """Called before streaming starts"""
    
    def _stream_end(self):
        """Called once streaming finished.
        @note called after our elapsed and bytes state was updated"""
        
    ## -- End Subclass Interface -- @}
        
    # -------------------------
    ## @name Interface
    # @{
    
    def set_stream(self, reader):
        """Set the given reader to be used on the next stream call
        @param reader a function with file.read semantics, thus it supports f(byes) -> read_bytes.
        Can also be a file name which will be opened for reading automatically
        @return self
        @note must be set before calling stream"""
        self._reader = reader
        return self
    
    def stream(self):
        """Stream all data yielded by the reader, and gather statistics
        @return self"""
        assert self._reader, "need a read to be set beforehand"
        st = time()
        self.bytes = 0
        self.elapsed = 0
        
        self._stream_begin()
        while True:
            chunk = self._reader(self.chunk_size)
            lchunk = len(chunk)
            self.bytes += lchunk
            
            if chunk:
                self._handle_chunk(chunk)
            # end call handler
            
            if lchunk < self.chunk_size:
                break
            # end abort loop if we didn't get anything
        #end endless loop
        
        self.elapsed = time() - st
        self._stream_end()
        
        return self
        
    ## -- End Interface -- @}
# end class Streamer


class HashStreamer(Streamer):
    """Generates any hash while streaming, and also provides a compression ratio
    """
    __slots__ = (
                    '_hash_constructor', # Function to create a new hash object
                    '_hasher',           # hasher instance, created by _hash_constructor
                    '_compressor',       # A function compressing an input string
                    'ratio',             # the average ratio of uncompressed size / compressed size, or 1 if
                                         # we didn't find lz4. 0.0 if we don't have the value
                    '_log',              # if set, extra progress is logged
                )
    
    def __init__(self, constructor, compressor = None):
        """Initialize this instance with the hash algorithm to use"""
        super(HashStreamer, self).__init__()
        self._log = None
        self._hash_constructor = constructor
        self._compressor = compressor
        self.ratio = None
    
    def _stream_begin(self):
        """Intitialize our hasher"""
        super(HashStreamer, self)._stream_begin()
        self._hasher = self._hash_constructor()
        self.ratio = None
        
    def _handle_chunk(self, chunk):
        super(HashStreamer, self)._handle_chunk(chunk)
        self._hasher.update(chunk)
        if self._compressor:
            ratio = len(chunk) / float(len(self._compressor(chunk)))
            # handle first chunk
            if self.ratio is None:
                self.ratio = ratio
            else:
                self.ratio = (self.ratio + ratio) / 2.0
            # end compute average, properly
        # end handle compression
        if self._log:
            self._log.info("Hashed %s", int_to_size_string(self.bytes))
        # end handle logging
        
    def _stream_end(self):
        """On-demand progress"""
        super(HashStreamer, self)._stream_end()
        if self._log:
            _mb = mb(self.bytes)
            self._log.info("Done hashing %s in %.2f s (%.2f MB/s)", int_to_size_string(self.bytes), self.elapsed, _mb / self.elapsed)
        # end progress

    # -------------------------
    ## @name Interface
    # @{
    
    def digest(self):
        """@return the has as byte string"""
        return self._hasher.digest()
        
    def set_log(self, log):
        """Set a logger or None to enable or disabled progress printing
        @return self"""
        self._log = log
        return self
    
    ## -- End Interface -- @}

# end class Sha1Streamer


class FSStatSubCommand(IToolSubCommand, bapp.plugin_type(), ApplicationSettingsMixin):
    """Implements interaction with filesystem info caches"""
    __slots__ = ()

    _schema = KeyValueStoreSchema('itool', {'fs_stat' : {'db_url' : str}})
    
    # -------------------------
    ## @name Baseclass Configuration
    # @{
    
    name = 'fs-stat'
    description = 'Convert .csv caches into sql ones, and make simple general purpose queries'
    version = '0.1.0'
    
    
    ## Something we generally consider a big file
    big_file = 50 * 1024**2
    
    ## -- End Baseclass Configuration -- @}
    
    
    def setup_argparser(self, parser):
        super(FSStatSubCommand, self).setup_argparser(parser)
        
        config = self.settings_value()
        help = "create or update the given database with path information. It must exist already, even though a table can be specified with -t."
        help += "If this flag is not set either on the commandline or through the configuration, nothing will be done."
        parser.add_argument('-ud', '--update-database', dest='update_db', metavar='SQLALCHEMY_URL', 
                           type=Path, default=config.fs_stat.db_url, help=help)
        
        help = "The table to put the path information into. It defaults to 'entries'"
        parser.add_argument('-t', '--table-name', dest='table_name', metavar='TABLE', default='entries', 
                            type=Path, help=help)
        
        help = "If set, the database will be updated just by quering it's values, and verifying them against the filesystem."
        help += "For this to work, the database must already exist"
        help += "Can be combined with --from-directories or --merge, which means that the table will"
        help += "be updated if it exists, and created otherwise"
        parser.add_argument('-f', '--fast', dest='fast', action='store_true', 
                           default=False, help=help)
        
        help = "In --fast mode, allows to specify a 'where path like <path>' expression to update only a subset"
        help += "of all entries. This way, one can run the update more concurrently and more focussed."
        help += "NOTE: the required wildcards are automatically handled"
        parser.add_argument('-wpl', '--where-path-like', dest='where_like', metavar='ABS_PATH', default=None, 
                           type=Path, help=help)
        
        help = "If set, we will indices the newly created table for the columns that make sense."
        help += "This can save time when querying, but slows down updates. In short, you should know what you need."
        help += "It should be preferred to create indices to speed up particular queries, and when needed."
        parser.add_argument('-i', '--with-index', dest='with_index', action='store_true', 
                           default=False, help=help)
        
        help = "If set, a database will be built from the given input directories."
        help += "Those will be crawled and all files will be read in recursively to obtain size and sha1"
        parser.add_argument('-fd', '--from-directories', dest='directories', nargs='+', metavar='DIRECTORY', 
                           help=help)
        
        help = "At least one directory containing SQL files whose code should be added as a view to the table we create."
        parser.add_argument('-sql', '--sql-to-view-directories', dest='sql_directories', nargs='+', metavar='SQL_DIRECTORY', 
                           help=help)
        
        help = "Specify paths to sqlite database that are to be merged into the database that is to be updated."
        help += "Views are not transferred, but you can attach new ones using the -sql flag."
        help += "Mutually exclusive with --from-directories."
        parser.add_argument('-m', '--merge', dest='merge_paths', nargs='+', metavar='SQLITE_DB_FILE', 
                           help=help)

        help = "Causes all duplicate paths to be removed, keeping only the most recent sample"
        parser.add_argument('-rd', '--remove-duplicate-paths', dest='remove_duplicates', action='store_true', 
                            default=False)
        return self
        
    def execute(self, args, remaining_args):
        if args.update_db:
            return self._update_db(args)
        else:
            self.log().error("--update-database not set or configured")
            return self.ERROR
        # end handle input arguments
        return self.SUCCESS
        

    # -------------------------
    ## @name Command Handling
    # @{
    
    def _url_from_path(self, path):
        """@return sqlite url from the given filepath, or leave it the url it is"""
        if is_url(path):
            return path
        return "sqlite:///%s" % path

    def _fetch_record_iterator(self, connection, selector, window):
        """@return an iterator which uses a window to retrieve 'window' amount of items based on the selector statement.
            It yields a cursor that should be iterated to obtain rows
        @note we cannot stop the iteration - instead you have to check if the amount of rows is smaller than
        the window size and abort yourself.
        """
        cur_window = 0
        log = self.log()
        while True:
            try:
                fst = time()
                log.info("Fetching %i records ... ", window)
                cursor = connection.execute(selector.limit(window).offset(cur_window))
                felapsed = time() - fst
                # Now we know how many entries we actually fetched, lets inform about speed
                log.info("FETCHED %i (or less) records in %.2fs (%.2f records/s), Window from %i to %i)", window, felapsed, window / felapsed, cur_window, cur_window + window)
                cur_window += window
                yield cursor 
            except Exception:
                log.error("Assumably, the schema we are using is not compatible with the one the database has. Update the database and try again", exc_info = True)
                raise
            # end handle exception
        # end select loop

    def _remove_duplicates(self, connection, fsitem):
        """remove all duplicate paths, keeping only the most recent entry
        @param connection to use, we will not close it's
        @param fsitem table meta data 
        @return amount of removed duplicates"""
        selector = select([fsitem.c.id, fsitem.c.path], order_by=[fsitem.c.path, fsitem.c.id.desc()])
        deletor = fsitem.delete().where(fsitem.c.id == bindparam('rid'))

        window = 1000000
        last_path = None
        deletions = list()
        log = self.log()

        nr = 0
        for cursor in self._fetch_record_iterator(connection, selector, window):
            nri = 0 # num rows in iteration
            for row in cursor:
                nri += 1
                nr += 1

                rid, path = row

                if path == last_path:
                    # mark entry for deletion
                    deletions.append({'rid' : rid})
                else:
                    last_path = path
                # end track last path
            # end for each row
            if nri < window:
                break
            # end abort cursor loop
        # end for each cursor

        if deletions:
            st = time()
            connection.execute(deletor, deletions)
            elapsed = time() - st
            log.info("Removed %i duplicate entries in %fs (%f deletions/s)", 
                        len(deletions), elapsed, len(deletions) / elapsed)
        else:
            log.info("No duplicates found")
        # end handle duplicates

        return nr
        
    def _fast_update_database(self, engine, args):
        """Update all data contained in the given engine quickly, see --fast
        @return number of processed records"""
        nr = 0
        st = time()
        log = self.log()
        progress_every = 5000
        stats_info_every = 500
        commit_every_seconds = 30
        commit_every_records = 15000
        time_of_last_commit = time()
        connection = engine.connect()
        meta = MetaData(engine, reflect=True)
        fsitem = meta.tables[args.table_name]
        insert = fsitem.insert()
        update = fsitem.update().where(fsitem.c.id == bindparam('rid')).values( path = bindparam('path'),
                                                                               size = bindparam('size'),
                                                                               atime = bindparam('atime'),
                                                                               ctime = bindparam('ctime'),
                                                                               mtime = bindparam('mtime'),
                                                                               uid = bindparam('uid'),
                                                                               gid = bindparam('gid'),
                                                                               nblocks = bindparam('nblocks'),
                                                                               nlink = bindparam('nlink'),
                                                                               mode = bindparam('mode'),
                                                                               ldest = bindparam('ldest'),
                                                                               sha1 = bindparam('sha1'),
                                                                               ratio = bindparam('ratio')
                                                                            )
        
        # NOTE: this selector assures we only get the latest version of a file, based on the modification time !
        selector = select([fsitem.c.id,
                           fsitem.c.path,
                           fsitem.c.size,
                           fsitem.c.atime,
                           fsitem.c.ctime,  # marker to see if something is deleted
                           fsitem.c.mtime,
                           fsitem.c.uid,
                           fsitem.c.gid,
                           fsitem.c.nblocks,
                           fsitem.c.nlink,
                           fsitem.c.mode,
                           fsitem.c.ldest,
                           fsitem.c.sha1,
                           fsitem.c.ratio], order_by=[fsitem.c.path, fsitem.c.id.desc()])
        
        if args.where_like:
            selector = selector.where(fsitem.c.path.like(args.where_like + '%'))
        # end append where clause
        
        
        def progress():
            elapsed = time() - st
            log.info("Checked %i files in %.2fs (%.2f files/s)", nr, elapsed, nr / elapsed)
        # end
        
        join = os.path.join
        isabs = os.path.isabs
        dirname = os.path.dirname
        basename = os.path.basename
        streamer = HashStreamer(hashlib.sha1, lz4dumps)
        ## A mapping from directory names to all of its files (as names)
        dir_entries = dict()
        
        # A list of sql operators that will update particular entries. They are executed all at once
        # Must include the ID
        updates = list()
        total_num_updates = 0
        modified_count = 0
        added_count = 0
        deleted_count = 0
        last_path = None
        # The window is critical - it is slow for the server, and each query is like a new complete query
        # where only a subset is sent (due to the ordering)
        # Additionally, if there are many changes, we will change the database during iteration, which will
        # basically give us part of the same files (if not the same files) back on the next query, which
        # makes us even more inefficient. Therefore we use memory to our advantage, and use 1mio entries
        # by default. This needs about 1GB of memory, but reduces the amount of queries considerably 
        # especially on large database
        window = 1000*1000
        cur_window = 0
        shortest_path = None
        len_shortest_path = 100000000
        
        for cursor in self._fetch_record_iterator(connection, selector, window):

            nri = 0 # num rows in iteration
            for row in cursor:
                # NOTE: We are getting multiple entries, sorted by the latest one, for the same path
                # We prune all paths of a kind have seen so far
                # Can be files or directories
                nri += 1
                nr += 1
                rid, path, size, atime, ctime, mtime, uid, gid, nblocks, nlink, mode, ldest, sha1, ratio = row
                if not isabs(path) or path == last_path:
                    continue
                # end skip relative paths !
                
                last_path = path
                ascii_path = to_ascii(path)
                
                # NOTE: I know, this is killing us, as we will grow rather large by keeping all that data
                # But I know no other way except for processing directories while we are going.
                # As files and directories will be mixed, it is not too easy though to figure this out.
                # For now, we just go for it and let the CPU/Memory burn
                directory = dirname(path)
                if directory not in dir_entries:
                    dir_entries[directory] = set()
                # end count dirs
                dir_entries[directory].add(basename(path))
                
                # Make sure we don't forget to set the actual directory - otherwise 
                if isdir(mode):
                    dir_entries.setdefault(path, set())
                # end add each directory that is a directory
                
                # Find the root path, which should be the origin of it all, and ignore it when
                # finding added items. It's definitely the shortest one
                if len(directory) < len_shortest_path:
                    shortest_path = directory
                    len_shortest_path = len(directory)
                # end keep shortest path
                
                try:
                    # For some reason, this doesn't get our unicode as it tries to use ascii to deal with it
                    # NOTE: We could know the file was deleted by checking fsitem.c.ctime is None, but 
                    # we check anyway because it could be re-created.
                    stat = lstat(ascii_path)
                except OSError:
                    # DELETION
                    ##########
                    # This marks a deletion - we just keep the time of deletion, which is the time when we 
                    # noticed it ! Not the actual one
                    # It didn't exist, but only append this info if we didn't know about that before
                    if ctime is not None:
                        # have to write an entire record, otherwise changes and deletions go out of sync
                        updates.append({    'rid' : rid,
                                            'path':     path,
                                            'size' :    0,
                                            'atime' :   atime,
                                            'ctime' :   None,
                                            'mtime' :   seconds_to_datetime(time()),
                                            'uid' : uid,
                                            'gid' : gid,
                                            'nblocks' : nblocks,
                                            'nlink' : nlink,
                                            'mode' : mode,
                                            'ldest' : ldest,
                                            # Keep sha as last known contents ! This allows to track deletion even
                                            # renames and deletions
                                            'sha1' : sha1,
                                            'ratio': ratio
                                       })
                        deleted_count += 1
                        if deleted_count % stats_info_every == 0:
                            log.info("Found %i DELETED paths", deleted_count)
                        # end handle deleted
                    # end handle deletions
                else:
                    # MODIFICATION
                    ###############
                    # File could have been deleted and re-created
                    # We don't know it was an addition (due to previous deletion), but the dataset is the same
                    # so people can figure it out later
                    # ordered by likeliness
                    if  seconds_to_datetime(stat.st_mtime) != mtime or\
                        size != stat.st_size                        or\
                        uid != stat.st_uid                          or\
                        gid != stat.st_gid                          or\
                        mode != stat.st_mode                        or\
                        nlink != stat.st_nlink                      or\
                        (islink(stat.st_mode) and readlink(ascii_path) != ldest):
                        
                        # NOTE: we are lazy here and say, for now, that the size must change to justify 
                        # taking another sha. Otherwise we assume that it's just any other change, which we will
                        # put into the database in the form of a new commit, of course.
                        if self._append_path_record(updates, path, streamer, log, stat,
                                                    size == stat.st_size and (sha1, ratio) or None):
                            # add the rid to have everything we need for the update
                            updates[-1]['rid'] = rid
                            modified_count += 1
                            if modified_count % stats_info_every == 0:
                                log.info("Found %i MODIFIED paths", modified_count) 
                            # end show information
                        # end handle modification
                    # end handle modification 
                #end handle deleted file
                
                if nr % progress_every == 0:
                    progress()
                #end handle progress
                
                if len(updates) >= commit_every_records or time() - time_of_last_commit >= commit_every_seconds:
                    total_num_updates += len(updates)
                    self.do_execute_records(connection, update, updates, log, st, total_num_updates)
                    time_of_last_commit = time()
                #end handle executions
            # end for each file in database windows
            cursor.close()
            
            # Is the database depleted ?
            if nri < window:
                break
            # end handle window
        # end for each cursor
        
        progress()
        total_num_updates += len(updates)
        self.do_execute_records(connection, update, updates, log, st, total_num_updates)
        
        ########################
        # HANDLE ADDITIONS ###
        ####################
        # We iterate all actual directories and their entries as known to the database
        # Now we just have to compare and only check for additions
        new_records = list()
        def list_dir_safely(dir_ascii):
            """@return entries of an empty tuple() if the listing failed"""
            try:
                return os.listdir(dir_ascii)
            except OSError:
                # ignore added dirs which might already be gone
                log.warn("Couldn't access '%s' when trying to add it", dir_ascii)
                return tuple()
            # end handle exception
            
        # We can't assign a variable in an outside scope, so we have to make it an array
        last_commit_time = [time()]
        def append_records_recursive(path, added_count):
            """Find all entries recursively in path and append them
            @param path directory or path
            @return amount of added items"""
            # no matter what, add the entry
            if self._append_path_record(new_records, path, streamer, log):
                added_count += 1
                if added_count % stats_info_every == 0:
                    log.info("Found %i ADDED paths", added_count)
                # end info printing
                if len(new_records) >= commit_every_records or time() - last_commit_time[0] >= commit_every_seconds:
                    self.do_execute_records(connection, insert, new_records, log, st, added_count)
                    last_commit_time[0] = time()
            # end handle path
            
            path_ascii = to_ascii(path)
            if os.path.isdir(path_ascii):
                entries = list_dir_safely(path_ascii)
                for entry in entries:
                    added_count = append_records_recursive(join(path, entry), added_count)
                #end for each entry to check 
            # end entries
            return added_count
        # end recursion helper
        
        
        # Remove shortest directory, which was generated from the directory of our root !
        # NOTE: if there was no root, this is false alarm
        try:
            del(dir_entries[shortest_path])
        except KeyError:
            pass
        # end ignore root not in dirlist
        
        log.info("About to check %i directories for added entries ...", len(dir_entries))
        for dir, entries in dir_entries.iteritems():
            added = set(list_dir_safely(to_ascii(dir))) - entries
            for added_entry in added:
                added_count = append_records_recursive(join(dir, added_entry), added_count)
        #end for each directory to check
        
        if new_records:
            log.info("Committing remaining %i new records", len(new_records))
            self.do_execute_records(connection, insert, new_records, log, st, added_count)
        # end commit new records
        connection.close()
        
        elapsed = time() - st
        log.info("== Statistics ==")
        log.info("%5i ADDED", added_count)
        log.info("%5i MODIFIED", modified_count)
        log.info("%5i DELETED", deleted_count)
        log.info("================")
        log.info("Updated %i entries in %.2fs (%.2f entries/s)", total_num_updates, elapsed, total_num_updates / elapsed) 
        
        return nr
    
    
    def _append_path_record(self, records, path, streamer, log, ex_stat = None, digest_ratio = None):
        """Append meta-data about the given path to the given list of records
        @param stat if you have received the stat already, we will not get it again
        @param digest_ratio if not None, we will use the given digest and ration  instead of creating our own
        @return stat structure of the path, or None if the path could not be read"""
        # minimize file access
        try:
            ascii_path = to_ascii(path)
            stat = ex_stat or lstat(ascii_path)
            
            if digest_ratio:
                digest, ratio = digest_ratio
            else:
                digest, ratio = None, None
            # end handle digest_ratio
            
            ldest = None
            fd = None
            
            
            
            if islink(stat.st_mode):
                # Don't follow symlinks as this tricks us into thinking we have duplicates.
                # Hower, we would also have to check for hardlinks, but tracking those 
                # can easliy cost too much memory. Hardlinks are rare anyway, so its okay.
                ldest = unicode(readlink(ascii_path))
            elif isreg(stat.st_mode) and not digest:
                fd = os.open(ascii_path, os.O_RDONLY)
            # end open file
        except OSError:
            log.error("Could not stat or open '%s' - skipping", ascii_path, exc_info=False)
            return None
        # end skip failing file
        
        
        if fd is not None:
            try:
                extra_progress = stat.st_size >= self.big_file
                if extra_progress:
                    log.info("Streaming %s file at '%s'", int_to_size_string(stat.st_size), ascii_path)
                # end extra logging
                
                try:
                    digest = streamer.set_stream(lambda size: os.read(fd, size))\
                                     .set_log(extra_progress and log or None)\
                                     .stream()\
                                     .digest()
                    ratio = streamer.ratio
                except IOError:
                    log.error("Failed to stream file '%s' - skipping", ascii_path, exc_info=True)
                    return None
                # end handle io errors gracefully
            finally:
                os.close(fd)
            # end assure we close the file
        # end handle symlink
        
        
        try:
            path = unicode(path)
        except Exception:
            log.error("Failed to handle encoding of path '%s' - skipping", ascii_path, exc_info=True)
            return None
        # end ignore unicode conversion errors
        
        # symlinks have a null-digest, which is why they are symlinks.
        # NOTE: We don't care about their contents, it's just a filename and 
        # we don't has it, as we are not interested about it's contents
        records.append({
                            'path' : path,
                            'size' : stat.st_size,
                            'atime': seconds_to_datetime(stat.st_atime),
                            'ctime': seconds_to_datetime(stat.st_ctime),
                            'mtime': seconds_to_datetime(stat.st_mtime),
                            'uid'  : stat.st_uid,
                            'gid'  : stat.st_gid,
                            'nblocks' : stat.st_blocks,
                            'nlink': stat.st_nlink,
                            'mode': stat.st_mode,
                            'ldest' : ldest,
                            'sha1' : digest,
                            'ratio' : ratio
                       })
            
        return stat
    
    
    def do_execute_records(self, connection, statement, records, log, overall_start_time = None, total_num_records = None):
        """Execute an sql statement on a given list of record dictionaries using a connection, provide status information into a give log.
        Overall_start_time is the time we took so far, in total, which makes us emit more information
        about overall performance.
        If the commit fails, we will rollback, but won't fail.
        Additionally we handle unicode errors relatively gracefully, by removing problematic entries and retrying
        @param connection an established connection to use for the transaction
        @param statement the sql statement
        @param records a list of dics of records to create. Information that is missing will be null
        @param log  a logger instance
        @param overall_start_time time at which the entire operation started (i.e. commandline invocation)
        @param total_num_records total amount of processed records since program invocation
        @note clears records in any case to prevent them from being re-inserted"""
        if not records:
            # prevent error on zero-insert
            return
        # end handle no value
        
        est = time()
        num_records = len(records)
        log.info("Committing %i records ...", num_records)
        try:
            with connection.begin() as transaction:
                try:
                    connection.execute(statement, records)
                    # keep what we have
                    transaction.commit()
                except UnicodeEncodeError, err:
                    # In this case, we can rescue the ship and just have to fix the records that failed to encode
                    # This is some weird issue, and it's just required to handle it so that 
                    log.warn("Encountered unicode error, fix + retry ...")
                    
                    # This works for mysql databases, and we try to do what it does to find
                    # the offending records
                    encoding = connection.connection.connection.character_set_name()
                    
                    new_records = list()
                    for record in records:
                        try:
                            record['path'].encode(encoding)
                            # reset ID, otherwise we will have duplicates at some point while merging at least
                            record['id'] = None
                            new_records.append(record)
                        except UnicodeEncodeError:
                            log.warn("Dropped record '%s'", record['path'].encode('utf-8')) 
                        # end handle exception
                    # end for each record
                    
                    # and retry
                    log.info("Retry commit with %i of %i records ...", len(new_records), len(records))
                    connection.execute(statement, new_records)

                    # When that happened, we will get a row with null values ! This needs to be cleaned
                    # Currently this happens at the end of the operation
                except Exception:
                    transaction.rollback()
                    log.error("Transaction failed and was rolled back - will keep going", exc_info=True)
                    return
                #end handle errors
            # end with transaction
            elapsed = time() - est
            log.info("Committed %i records in %.2fs (%.2f records/s)", num_records, elapsed, num_records / elapsed)
            if overall_start_time is not None and total_num_records is not None:
                elapsed = time() - overall_start_time
                log.info("Total time to process %i records: %.2fs (%.2f records/s)", total_num_records, elapsed, total_num_records / elapsed)
            # end handle additional logging event
        finally:
            del(records[:])
        # end assure records are cleared
    # end utility
    
    
    def _update_db(self, args):
        """Update the sqlite database database
        @return error code"""
        log = self.log()
        
        num_sources = bool(args.directories) + bool(args.merge_paths)
        if num_sources > 1:
            raise AssertionError("Cannot use --from-directories or --merge together")
        elif num_sources and args.remove_duplicates:
            raise AssertionError("--remove-duplicate-paths cannot be used in conjunction with any source")
        elif not (args.fast or args.remove_duplicates) and num_sources == 0:
            raise AssertionError("Specify at least one of the flags specifying from where to update the database")
        # end assure consistency
        
        #############
        # INIT DB ##
        ###########
        path = args.update_db
        engine = create_engine(self._url_from_path(path))
        meta = None
        # Assume file exists
        if is_url(path) or path.isfile():
            meta = MetaData(engine, reflect=True)
        # end handle file exists

        if not meta or args.table_name not in meta.tables:
            if args.fast:
                log.warn("Database didn't exist yet - fast implicitly disabled")
                args.fast = False
                if num_sources == 0:
                    raise AssertionError("Require at least one initial data source, either --from-directories or --merge")
                # end handle logic
            # end handle fast
            if args.remove_duplicates:
                raise AssertionError("Cannot remove duplicates on non-existing table")
            # end handle remove duplicates
            
            meta = fsstat_schema.meta
            fsstat_schema.record.name = args.table_name
            meta.bind = engine
            meta.create_all()
            log.info("initalized database at %s", path)
            fsitem = fsstat_schema.record
            # assure we have the meta-data with the proper name - renaming the table before we create_all
            # is kind of a hack
            meta = MetaData(engine, reflect=True)
        else:
            if args.with_index:
                log.info("Cannot create index on exiting table without additional logic - turning index creation off")
            # end
            args.with_index = False
            
            fsitem = meta.tables[args.table_name]
            log.info("Updating database '%s' at '%s'", path, args.table_name)
        # end initialize table
        
        
        strip = str.strip
        basename = os.path.basename
        connection = engine.connect()
        insert = fsitem.insert()
        
        st = time()
        nr = 0          # num records handled
        records = list()
        
        ########################
        # REMOVE DUPLICATES ###
        ######################
        if args.remove_duplicates:
            nr = self._remove_duplicates(connection, fsitem)
        ######################
        # FAST UPDATE ####
        ###############
        elif args.fast:
            nr = self._fast_update_database(engine, args)
        ###########################
        ## DIRECTORY CRAWLING ####
        #########################
        elif args.directories:
            
            streamer = HashStreamer(hashlib.sha1, lz4dumps)
            join = os.path.join
            normalize = os.path.normpath
            totalbcount = 0 # total amount of bytes processed
            
            lct = time()
            progress_every = 500
            commit_every_fcount = 15000
            commit_every_seconds = 1 * 60   ## commits per minute
            
            def progress():
                elapsed = time() - st
                log.info("Processed %i files with %s in %.2fs (%.2f files/s | %s MB/s)", nr, int_to_size_string(totalbcount), elapsed, nr / elapsed, mb(totalbcount) / elapsed)
            # end
            
            
            for directory in args.directories:
                if not os.path.isdir(directory):
                    log.error("Skipped non-existing directory '%s'", directory)
                    continue
                # end handle failed directory acccess
                
                # normalize to prevent extra stuff
                directory = normalize(directory) 
                for root, dirs, files in os.walk(directory, followlinks=False):
                    # NOTE: We also take directories, as it allows to find directories with many files, or with
                    # no files (empty directories). Also, we can optimize updates that way
                    # Just to also handle root ! It must be in the database, otherwise we can never
                    # handle additions correctly, at least not for the root folder
                    chains = [files, dirs]
                    if root is directory:
                        # an empty string joined with root, is root
                        chains.insert(0, [''])
                    #end handle root
                    for filename in chain(*chains):
                        nr += 1
                        # only join if we are not seeing the root. Otherwise we get a slash appended
                        # Which is something we really don't want as it could hinder later updates
                        path = filename and join(root, filename) or root 
                        stat = self._append_path_record(records, path, streamer, log)
                        if stat:
                            totalbcount += stat.st_size
                                
                            if nr % progress_every == 0:
                                progress()
                            # end show progress
                        # end managaed to handle file
                        
                        if time() - lct >= commit_every_seconds or nr % commit_every_fcount == 0:
                            lct = time()
                            progress()
                            self.do_execute_records(connection, insert, records, log, st, nr)
                        # end commit
                # end for each file
            # end for each directory to traverse
            # final execute
            progress()
            self.do_execute_records(connection, insert, records, log, st, nr)
        #########################
        ## Database Merges  ####
        ######################
        elif args.merge_paths:
            ## Commit this amount of records at once
            commit_count = 100000
            
            def progress():
                elapsed = time() - st
                log.info("Inserted %i records in %.2fs (%.2f records/s)", nr, elapsed, nr / elapsed)
            # end
            
            for merge_path in args.merge_paths:
                merge_path = Path(merge_path)
                
                if not is_url(merge_path) and not merge_path.isfile():
                    log.error("Database at '%s' didn't exist - skipping", merge_path)
                    continue
                # end for each path
                
                log.info("Merging DB at '%s' ...", merge_path)
                merge_engine = create_engine(self._url_from_path(merge_path))
                mcon = merge_engine.connect()
                md = MetaData(merge_engine, reflect=True)
                
                
                try:
                    for table in md.tables.itervalues():
                        # If id is part of it, and we rollback because of a unicode error, the counter
                        # will be offset and we cannot commit anymore. Just let it be done automatically, no
                        # matter what
                        column_names = [c.name for c in table.columns if c != 'id']
                        try:
                            cursor = mcon.execute(select([table]))
                            
                            # We assume the cursor deals with the query efficiently, and doesn't really fetch everything
                            while True:
                                fst = time()
                                log.info("Fetching %i '%s' records from '%s' ...", commit_count, table.name, merge_path)
                                
                                rows = cursor.fetchmany(commit_count)
                                records.extend(dict(zip(column_names, row)) for row in rows)
                                
                                elapsed = time() - fst
                                log.info("Fetched %i records in %.2fs (%.2f records/s)", len(records), elapsed, len(records) / elapsed)
                                
                                nr += len(records)
                                must_break = len(records) < commit_count
                                
                                ##############
                                self.do_execute_records(connection, insert, records, log, st, nr)
                                progress()
                                ##############
                                
                                # Did we get enough ?
                                if must_break:
                                    break
                                # end check for end of iteration
                            #end endless loop
                        finally:
                            cursor.close()
                    # end for each table to merge
                finally:
                    mcon.close()
                # end assure we close resources
            # end for each merge path
        else:
            raise AssertionError("Reached unexpected mode") 
        # end handle mode of operation
        
        ##############################
        # CREATE INDICES AND VIEWS ##
        ############################
        if args.with_index:
            # Create one index per column, which allows fast searches over it
            # Create a custom one that speeds up our common search group by path, order by path, mtime.
            for col in fsitem.columns:
                # id is primary, and thus already indexed
                # path is too big - it needs to be hashed to be useful in an actual index
                # file as well
                if col in (fsitem.c.id, fsitem.c.path, fsitem.c.sha1):
                    continue
                # end handle index creation
                ist = time()
                log.info("Creating index for columns '%s' ...", col)
                try:
                    Index('idx_%s_%s' % (fsitem.name, col.name), col).create(engine)
                except Exception:
                    log.error("Index creation failed", exc_info=True)
                else:
                    elapsed = time() - ist
                    log.info("Created index with %i entries in %.2fs (%.2f entries/s)" % (nr, elapsed, nr / elapsed)) 
                # end handle creation errors
            # end for each index to create
        # end handle index creation
        
        if args.sql_directories:
            for sql_dir in args.sql_directories:
                sql_dir = Path(sql_dir)
                for sql_file in sql_dir.files(pattern = "*.sql"):
                    try:
                        transaction = connection.begin() 
                        log.info("Creating view from '%s'", sql_file)
                        connection.execute(sqlite_view_from_file(sql_file))
                        transaction.commit()
                    except Exception:
                        transaction.rollback()
                        log.error("Failed to create view for file '%s' - it might have existed - skipping", sql_file)
                        continue
                    # end handle transaction per sql view
                # end for each file
            # end for eeach sqldir
        # end have sql directories

        # FINAL CLEANUP
        ################
        # If there were unicode errors, we end up having a row with a null-path. This breaks our code
        # Lets keep the data consistent instead of altering code
        dst = time()
        connection.execute(fsitem.delete().where(fsitem.c.path == None))
        log.info("Cleaned dataset after (possible) unicode errors in %fs", time() - dst)

        connection.close()
        
        ##################
        # FINAL INFO ###
        ###############
        elapsed = time() - st
        log.info("Overall time to process %i records is %.2fs (%.2f records/s)", nr, elapsed, nr / elapsed)
        log.info("File written to %s", Path(args.update_db).abspath())
        
        return self.SUCCESS
        
    
    ## -- End Command Handling -- @}
# end class FSStatSubCommand
