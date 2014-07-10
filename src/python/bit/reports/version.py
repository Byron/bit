#-*-coding:utf-8-*-
"""
@package bit.reports.version
@brief A report showing filesystem versions

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['VersionReportGenerator']

import sys
import os
import re
from time import time
import marshal
import lz4
from datetime import datetime
from itertools import chain

from butility import Path
from .base import ReportGeneratorBase
from bit.retention import RetentionPolicy
from bit.utility import (int_to_size_string,
                           delta_to_tty_string,
                           seconds_to_datetime,
                           datetime_to_seconds,
                           ravg,
                           rsum,
                           DistinctStringReducer)

from bit.bundler import (Bundler,
                           VersionBundleList,
                           VersionBundle)

from bcmd import InputError

from sqlalchemy import (create_engine,
                        MetaData,
                        select)




# ==============================================================================
## @name Utility Functions
# ------------------------------------------------------------------------------
## @{

def seconds_to_delta_string(date_seconds):
    """@return a string representing the given time (in past) in seconds in time relative to current time"""
    return delta_to_tty_string(now - seconds_to_datetime(date_seconds))

now = datetime.now()
to_s = datetime_to_seconds
dirname = os.path.dirname

## -- End Utility Functions -- @}


# ==============================================================================
## @name Utility Types
# ------------------------------------------------------------------------------
## @{

class StatVersionBundle(VersionBundle):
    """Adds aggregation support for our meta data"""
    __slots__ = (
                    'removed', # True if we are being removed
                )

    # -------------------------
    ## @name Configuration
    # @{
    
    def getter(i):
        def index_at(item):
            return item[1][i]
        return index_at
    # end get generator

    def disk_size(item):
        return int(item[1][0] / item[1][4])
    # end 

    _aggregator = ( ('disk_size', rsum, disk_size),
                    ('logical_size', rsum, getter(0)),
                    ('avg_created', ravg, getter(1)),
                    ('avg_modified', ravg, getter(2)),
                    ('min_created', min, getter(1)),
                  )

    del disk_size
    del getter

    ## -- End Configuration -- @}

    def __init__(self, *args):
        self.removed = False
        
    @property
    def num_files(self):
        return len(self)        

# end class StatVersionBundle

class StatVersionBundleList(VersionBundleList):
    """A bundle list which supports aggregation of our particular meta-data"""
    __slots__ = ('prefix',      # prefix common to all of our versions
                )

    def __init__(self):
        self.prefix = ''

    def getter(name):
        def attr_at(bundle):
            return getattr(bundle, name)
        return attr_at
    # end getter

    _aggregator = list()
    for info in StatVersionBundle._aggregator:
        _aggregator.append((info[0], info[1], getter(info[0])))
    # end for each attribute to accumulate

    # Special attributes based on removal of bundle
    _aggregator.extend((('num_files', rsum, lambda b: b.num_files),
                        ('num_del_versions', rsum, lambda b: int(b.removed)),
                        ('num_del_files', rsum, lambda b: b.removed and b.num_files or 0 ),
                        ('freed_disk_size', rsum, lambda b: b.removed and b.disk_size or 0)
                        ))

    del getter

    def __hash__(self):
        return hash(self.prefix)

    def __str__(self):
        return self.prefix

    # -------------------------
    ## @name Interface
    # @{

    @property
    def num_versions(self):
        return len(self)

    ## -- End Interface -- @}

# end class StatVersionBundleList


class FilteringVersionBundler(Bundler):
    """A bundler which rebuilds using our types, and which applies a filter in the process"""
    __slots__ = ('config')

    BundleListType = StatVersionBundleList
    BundleType = StatVersionBundle

    def __init__(self, config):
        super(FilteringVersionBundler, self).__init__()
        self.config = config

    def _keep_prefix(self, prefix):
        if self.config.prefix_include_regex:
            return self.config.prefix_include_regex.match(prefix) is not None
        if self.config.prefix_exclude_regex:
            return self.config.prefix_exclude_regex.match(prefix) is None
        return True

    def _keep_item(self, item):
        if self.config.path_include_regex:
            return self.config.path_include_regex.match(item[0]) is not None
        if self.config.path_exclude_regex:
            return self.config.path_exclude_regex.match(item[0]) is None
        return True

    def _dict_to_bundle_list(self, prefix, bundle_dict):
        """Assure we apply retention per-version-bundle list"""
        if not self.config.retention_policy and self.config.keep_latest_version_count < 0:
            bundle_list = super(FilteringVersionBundler, self)._dict_to_bundle_list(prefix, bundle_dict)
        else:
            # MARK BUNDLES FOR DELETION
            ###########################
            # NOTE: When using the policy, it is very important that newer versions are also newer regarding the date.
            # This is why we resort to the min_created attribute, the youngest item counts (just in case people overwrite versions)
            bundle_list = self.BundleListType()
            if self.config.retention_policy:
                samples, removed_samples = self.config.retention_policy.filter(time(),
                                                                               ((seconds_to_datetime(b.min_created), b) for b in self._iter_bundles_in_dict(bundle_dict)),
                                                                               ordered=False)
                for t, b in removed_samples:
                    b.removed = True
                # end for each sample
                bundle_list.extend(sorted((s[1] for s in chain(samples, removed_samples)), key=lambda b: b.version))
            else:
                bundle_list.extend(self._iter_bundles_in_dict(bundle_dict))
                bundle_list.sort(key=lambda b: b.version)

                # can be negative, yielding nothing to iterate on
                for vid in xrange(len(bundle_list) - self.config.keep_latest_version_count):
                    bundle_list[vid].removed = True
                # end for each version to remove
            # end handle policy or stupid keep count
        # end handle bundle list conversion

        bundle_list.prefix = prefix
        return bundle_list

# end class FilteringVersionBundler

## -- End Utility Types -- @}



class VersionReportGenerator(ReportGeneratorBase, Plugin):
    """Generates information about versions used in filesystems"""
    __slots__ = ()

    type_name = 'version'
    description = """A very powerful command which uses the nightly directory tree information available for
         each project to find all versioned assets within a project, filtering them as needed, to output a 
         report which can be used to delete old versions."""

    ORDER_ASC = 'ascending'
    ORDER_DESC = 'descending'

    sort_orders = (ORDER_ASC, ORDER_DESC)
    report_schema = (   ('prefix', StatVersionBundleList, lambda v: isinstance(v, str) and v or dirname(v.prefix), DistinctStringReducer()),
                        ('num_versions', int, str, rsum),
                        ('num_del_versions', int, str, rsum),
                        ('version_min', int, str),
                        ('version_max', int, str),
                        ('num_files', int, str, rsum),
                        ('num_del_files', int, str, rsum),
                        ('disk_size', int, int_to_size_string, rsum),
                        ('freed_disk_size', int, int_to_size_string, rsum),
                        ('logical_size', int, int_to_size_string, rsum),
                        ('avg_created', int, seconds_to_delta_string),
                        ('avg_modified', int, seconds_to_delta_string),
                    )

    _schema = ReportGeneratorBase._make_schema(type_name, dict(db_url=str(), # sqlalchemy url to database to use
                                                               table=str(),  # name of the table to use, compatible to fsstat
                                                               cache_path=Path(), # an optional path to a cache - auto-tried based on table name if set

                                                               prefix_include_regex=str(), # regular expression of prefix whitelist
                                                               prefix_exclude_regex=str(), # inverse of above
                                                               path_include_regex=str(), # regular expression of paths to whitelist
                                                               path_exclude_regex=str(), # inverse of above

                                                               sort_by=report_schema[0][0], # column by which to sort
                                                               sort_order=ORDER_ASC, # sort order

                                                               retention_policy=str(), # Standard retention policy
                                                               keep_latest_version_count=-1 # amount of newest versions to keep
                                                          ))

    # -------------------------
    ## @name Utilities
    # @{

    def _cache_path(self, name):
        """@return Path to cache file based on table name"""
        return Path(name + '_%04i-%02i-%02i.cache.lz4' % (now.year, now.month, now.day))

    def _serialize_db(self, db, path):
        """Serialize the given db fast
        @return size of cached data in bytes"""
        cache = lz4.dumps(marshal.dumps(db))
        open(path, 'wb').write(cache)
        return len(cache)

    def _deserialize_db(self, path):
        """@return the deserialized database, previusly written by _serialize_db()"""
        return marshal.loads(lz4.loads(open(path).read()))

    def _build_database(self, config):
        """@return our database ready to be used.
        It will be a list of tuples of (prefix, VersionBundleList) pairs
        Will load from cache or from an sql database (and building the cache in the process)"""
        if not config.cache_path:
            if not config.table:
                raise AssertionError("Please set either db_url and table or the cache_path to specify a data source")
            config.cache_path = self._cache_path(config.table)
            print >> sys.stderr, "Would use cache default at %s" % config.cache_path
        else:
            print >> sys.stderr, "Will attempt to use cache at %s" % config.cache_path
        # end handle cache_path

        db = None
        # prefer to use a cache
        if config.cache_path and config.cache_path.isfile():
            # LOAD EXISTING CACHE
            ######################
            st = time()
            db = self._deserialize_db(config.cache_path)
            elapsed = time() - st

            cstat = config.cache_path.stat()
            print >> sys.stderr, "Loaded cache of size %s from %s in %fs (%fMB/s)" % \
                                    (int_to_size_string(cstat.st_size), config.cache_path, elapsed, 
                                     (cstat.st_size / elapsed) / (1024**2))
        # end try loading cache
        elif config.db_url and config.table:
            # BUILD CACHE FROM DATABASE
            ############################
            print >> sys.stderr, "reading from database at '%s/%s'" % (config.db_url, config.table)

            engine = create_engine(config.db_url)
            mcon = engine.connect()
            md = MetaData(engine, reflect=True)

            if config.table not in md.tables:
                raise AssertionError("Table named '%s' didn't exist in database at '%s'" % (config.table, config.db_url))
            # end verify table exists

            progress_every = 40000
            def record_iterator():
                c = md.tables[config.table].c
                selector = select(  [c.path,
                                     c.size,
                                     c.ctime,
                                     c.mtime,
                                     c.mode,
                                     c.ratio], (c.ctime != None) & (c.mtime != None) & (c.sha1 != None)).order_by(c.path)

                st = time()
                for rid, row in enumerate(mcon.execute(selector)):
                    if rid % progress_every == 0:
                        elapsed = time() - st
                        print >> sys.stderr, "Read %i records in %fs (%f records/s)" % (rid, elapsed, rid / elapsed)
                    # end handle progress
                    yield (row[0], (row[1],
                                    to_s(row[2]),
                                    to_s(row[3]),
                                    row[4],
                                    row[5] or 1.0))
                # end for each row
            # end record iterator

            st = time()
            db = FilteringVersionBundler(config).bundle(record_iterator())
            print >> sys.stderr, "Extracted version %i bundled in %fs" % (len(db), time() - st)

            # store cache file
            st = time()
            cpath = self._cache_path(config.table)
            csize = self._serialize_db(db, cpath)
            elapsed = time() - st
            print >> sys.stderr, "Wrote cache with size %s to '%s' in %fs (%f MB/s)" % \
                                         (int_to_size_string(csize), cpath, elapsed, (csize / elapsed) / 1024**2)
        # end obtain raw database
        else:
            raise AssertionError("Could not build cache database - set db_url and table, cache_path, or table to use a default cache from previous run")
        # end handle cache or db url

        # DEBUILD RAW CACHE
        ####################
        # finally, rebuild and filter into our actual structure
        st = time()
        db = FilteringVersionBundler(config).rebuild_bundle(db)

        def prefix(t):
            return t[0]
        # end prefix getter

        def key_factory(attr):
            def meta_get(t):
                return getattr(t[1], attr)
            return meta_get
        # end factory

        # SORT INTO FLAT LIST
        #####################
        key_fun = config.sort_by == self.report_schema[0][0] and prefix or key_factory(config.sort_by)

        db = sorted(db.iteritems(), 
                    key = key_fun,
                    reverse = config.sort_order == self.ORDER_DESC)
        elapsed = time() - st

        print >> sys.stderr, "Filtered database in %fs" % elapsed
        return db

    def _sanitize_configuration(self, config):
        """@return configuration which is assured to have the correct type of values."""
        if config.prefix_include_regex and config.prefix_exclude_regex or \
           config.path_include_regex and config.path_exclude_regex or \
           config.retention_policy and config.keep_latest_version_count:
            raise InputError("Include- and exclude-regex are mutually exclusive.")
        # end assure mutual exclusivity

        for name in config.keys():
            if not name.endswith('regex') or not config[name]:
                continue
            # end
            try:
                config[name] = re.compile(config[name], re.IGNORECASE)
            except Exception, err:
                raise InputError("Couldn't compile regular expression at '%s': %s" % (name, str(err)))
            # end handle exception
        # end for each config value

        if config.retention_policy:
            config.retention_policy = RetentionPolicy(config.retention_policy)
        # end handle policy

        if config.sort_by not in list(t[0] for t in self.report_schema):
            raise InputError("sort_by must be one of our columns, like 'prefix'")
        # end handle sort by

        if config.sort_order not in self.sort_orders:
            raise InputError('sort_order must be one of %s' % ', '.join(self.sort_orders))
        # end handle sort order

        return config

    ## -- End Utilities -- @}

    # -------------------------
    ## @name Interface Implementation
    # @{

    def generate(self):
        report = self.ReportType(columns=self.report_schema)
        record = report.records.append
        config = self._sanitize_configuration(self.configuration())

        db = self._build_database(config)

        meta_attrs = [t[0] for t in self.report_schema][1:]
        for prefix, vlist in db:
            record((vlist, ) + tuple(getattr(vlist, attr) for attr in meta_attrs))
        # end for each entry to place

        record(report.aggregate_record())

        return report

    def generate_fix_script(self, report, writer):
        """Generate a script which removes files individually as well as empty folders"""
        # PREAMBLE
        ###########
        self.delete_script_safety_prefix(writer)

        # DELETE FILES
        ##############
        fcount = 0
        dcount = 0
        for rec in report.records:
            vlist = rec[0]
            if not isinstance(vlist, StatVersionBundleList):
                continue
            # end ignore aggregated record

            # Reduce size of script by using common prefix as directory
            dirs = set()
            lines = list()
            lines_append = lines.append
            fpb = 0         # files per bundle

            prefix_dir = dirname(vlist.prefix)
            lpd = len(prefix_dir)
            dirs.add(prefix_dir)

            lines_append('if cd "%s"; then\n' % prefix_dir)
            lines_append('    xargs -n 100 $prefix rm -vf <<FILES \n')
            for bundle in vlist:
                if not bundle.removed:
                    continue
                # end ignore if not for removal !

                for item in bundle:
                    fcount += 1
                    fpb += 1
                    file_relative = item[0][lpd+1:]
                    lines_append(file_relative + '\n')
                    fdir = dirname(file_relative)
                    if fdir:
                        dirs.add(fdir)
                # end for each item
            # end for each bundle
            lines_append('FILES\n')

            # DELETE DIRECTORIES
            #####################
            if fpb and dirs:
                lines_append('    xargs -n 50 $prefix rmdir --ignore-fail-on-non-empty <<DIRS 2>/dev/null\n')
                dcount += len(dirs)
                for dir in sorted(dirs, key=lambda d: len(d), reverse=True):
                    lines_append(dir + '\n')
                # end for each directory
                lines_append('DIRS\n')
            # end handle directories

            # this one ends the 'cd $prefix' clause
            lines_append('fi\n')

            if fpb:
                for line in lines:
                    writer(line)
                #end handle line
            # end only write if we found removable bundles in the version list
        #end for each version bundle to be removed
        
        writer('echo "Removed up to %i files and %i directories"\n' % (fcount, dcount))
        return True
    
    ## -- End Interface Implementation -- @}


    # -------------------------
    ## @name Interface
    # @{

    @classmethod
    def delete_script_safety_prefix(cls, writer):
        """Write a safety section which will make accidental deletion very hard"""
        magic='DOIT'
        writer('#!/bin/bash\n')
        writer('# First argument can be magic word "%s" to actually perform the deletion. It will be dry run otherwise\n' % magic)
        writer('if [[ $UID != 0 ]]; then \n')
        writer('    echo "must be root to execute this script"\n')
        writer('    exit 1\n')
        writer('fi\n\n')
        writer('read -p "This operation will delete files and folders. Are you sure ? yn [n]:" res\n')
        writer('if [[ $res != y ]]; then\n')
        writer('    echo "aborted by user"\n')
        writer('    exit 1\n')
        writer('fi\n')
        writer('prefix=echo\n')
        writer('if [[ "$1" = %s ]]; then\n' % magic)
        writer('    prefix=\n')
        writer('fi\n')
    
    ## -- End Interface -- @}

# end class VersionReportGenerator
