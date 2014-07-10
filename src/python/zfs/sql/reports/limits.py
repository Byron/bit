#-*-coding:utf-8-*-
"""
@package zfs.sql.reports.limits
@brief A report checking for limits on sizes and quota

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZLimitsReportGenerator']

from datetime import datetime
from time import time

from tx.core.kvstore import StringList
from bit.utility import (
                                size_to_int,
                                datetime_days_ago,
                            )
from .base import (
                    ZReportGenerator,
                    host_filter
                 )
from .. import (
                    ZPool,
                    ZDataset,
                )


class ZLimitsReportGenerator(ZReportGenerator, Plugin):
    """Find pools and or datasets that are above a certain metric limit"""
    __slots__ = ()

    type_name = 'limits'
    description = "This command is great if you want to list filesystems or snapshots which match a certain \
                   minimum or maximum value. In production, its main purpose is to list snapshots older than\
                   an amount of days, in order to reduce storage requirements."
    _schema = ZReportGenerator._make_schema(type_name, dict(max_pool_cap = 80.0, 
                                                            max_filesystem_cap = 80.0,
                                                            min_filesystem_avail_size = '0k',
                                                            min_snapshot_size = '100k',
                                                            min_filesystem_size = '100k',
                                                            snapshots_older_than_days = 180,
                                                            filesystems_older_than_days = 180,
                                                            hosts = StringList,
                                                            name_like = '%'
                                                            ))
    def generate(self):
        """See class description and schema for more information"""
        rep = self.ReportType()
        rep.columns.extend(self.report_schema)

        max_pool_cap,\
        fs_cap,\
        min_snapshot_size,\
        min_filesystem_size,\
        snapshots_older_than_days,\
        filesystems_older_than_days,\
        name_like,\
        hosts                       = ( self._config.max_pool_cap, 
                                        self._config.max_filesystem_cap,
                                        self._config.min_snapshot_size,
                                        self._config.min_filesystem_size,
                                        self._config.snapshots_older_than_days,
                                        self._config.filesystems_older_than_days,
                                        self._config.name_like,
                                        self._config.hosts)
        assert name_like, "Name matching must be at least '%%', got nothing"
        rappend = rep.records.append
        now = datetime.now()
        nows = time()
        percent_comment = '%s.cap > %.0f%%'

        # POOL CAPACITIES
        ###################
        if max_pool_cap <= 100.0:
            fill_capage = (ZPool.size - ZPool.free) / (ZPool.size * 1.0)
            percent_condition = fill_capage >= max_pool_cap / 100.0
            comment = percent_comment % (self.TYPE_POOL, max_pool_cap)
            for pool in host_filter(hosts, ZPool.host, self._session.query(ZPool).\
                                            filter(percent_condition).\
                                            filter(ZPool.name.like(name_like)).\
                                            order_by(fill_capage.desc())): 
                rec = [now - pool.updated_at, pool.host, pool.name, self.TYPE_POOL,'NA', 'NA', pool.size, pool.free, 
                        ((pool.size - pool.free) / float(pool.size)) * 100, comment]
                rappend(rec)
            # end for each that matches
        # end check enabled

        # FILESYSTEM CAPACITIES
        ########################
        if fs_cap <= 100.0:
            filesystem = ZDataset.avail != None
            fill_capage = 1.0 - (ZDataset.avail / (ZDataset.avail + ZDataset.used) * 1.0)
            percent_condition = fill_capage >= fs_cap / 100.0
            comment = percent_comment % (self.TYPE_FILESYSTEM, fs_cap)
            for fs in host_filter(hosts, ZDataset.host, self._session.query(ZDataset).\
                                                filter(filesystem).\
                                                filter(ZDataset.name.like(name_like)).\
                                                filter(percent_condition).\
                                                order_by(fill_capage.desc())):
                rec = [ now - fs.updated_at, fs.host, fs.name, self.TYPE_FILESYSTEM, fs.creation, now - fs.creation, fs.used, fs.avail, 
                        (1.0 - (fs.avail / float((fs.avail + fs.used)))) * 100.0, comment]
                rappend(rec)
            # end for each record
        # end check enabled

        # DATASETS TOO SMALL
        #####################
        # TODO: implement protection against deleting the last snapshot ! In case it's on, we must assure
        # the latest snapshot of a filesystem is not deleted. Could be a complex group + join
        def make_records(type_condition, condition, type_name, comment):
            for ds in host_filter(hosts, ZDataset.host, self._session.query(ZDataset).\
                                        filter(type_condition).\
                                        filter(ZDataset.name.like(name_like)).\
                                        filter(condition).\
                                        order_by(ZDataset.host)):
                rec = [now - ds.updated_at, ds.host, ds.name, type_name, ds.creation, now - ds.creation, 
                        ds.used, ds.avail or 0,
                        ds.avail and ((ds.used / float(ds.used + ds.avail)) * 100.0) or 100.0, comment]
                rappend(rec)
            # end for each snapshot
        # end make records utility

        min_size_fmt = '%s.size < %s'
        if min_snapshot_size:
            condition = ZDataset.used < size_to_int(min_snapshot_size)
            make_records(ZDataset.avail == None, condition, self.TYPE_SNAPSHOT, 
                            min_size_fmt % (self.TYPE_SNAPSHOT, min_snapshot_size))
        if min_filesystem_size:
            condition = ZDataset.used < size_to_int(min_filesystem_size)
            make_records(ZDataset.avail != None, condition, self.TYPE_FILESYSTEM, 
                            min_size_fmt % (self.TYPE_FILESYSTEM, min_filesystem_size))
        # end handle item sizes


        # DATASET AGE
        ##############
        if snapshots_older_than_days:
            condition = ZDataset.creation <= datetime_days_ago(nows, snapshots_older_than_days)
            make_records(ZDataset.avail == None, condition, self.TYPE_SNAPSHOT, 
                            '%s.creation older %id' % (self.TYPE_SNAPSHOT, snapshots_older_than_days))

        if filesystems_older_than_days:
            condition = ZDataset.creation <= datetime_days_ago(nows, filesystems_older_than_days)
            make_records(ZDataset.avail != None, condition, self.TYPE_FILESYSTEM, 
                            '%s.creation older %id' % (self.TYPE_FILESYSTEM, filesystems_older_than_days))
        # end ignore age if time is 0, which is essentially just no


        # FILESYSTEM FREE SPACE
        ########################
        min_filesystem_avail_size = size_to_int(self._config.min_filesystem_avail_size)
        if min_filesystem_avail_size:
            condition = ZDataset.avail >= min_filesystem_avail_size
            make_records(ZDataset.avail != None, condition, self.TYPE_FILESYSTEM, 
                            '%s.avail >= %s' % (self.TYPE_FILESYSTEM, self._config.min_filesystem_avail_size))
        # end ignore this value if zero
        

        # AGGREGATE
        #############
        self._aggregate_records(rep.records, now)
        return rep

    def generate_fix_script(self, report, writer):
        """Generate a script with progress and zfs destroy commands
        @note will only generate commands for one host"""
        mss = self._config.min_snapshot_size or 0
        if mss:
            mss = size_to_int(mss)
        # end handle type conversion
        fss = self._config.min_filesystem_size or 0
        if fss:
            fss = size_to_int(fss)
        # end handle type conversion
        sotd = self._config.snapshots_older_than_days
        date = datetime_days_ago(time(), sotd)

        def predicate(rec):
            tn = rec[3]
            ctime = rec[4] # creation delta time
            size = rec[6]

            # We don't know what's part of the Report, and why those values are there.
            # This is why we have to re-evaluate the inverse logic to prevent us from 
            # generating code that would mean something different
            return (tn == self.TYPE_SNAPSHOT   and (size < mss or ctime <= date)) or (tn == self.TYPE_FILESYSTEM and size < fss)
        # end predicate

        header_line = "SETTING UP ZFS DESTROY COMMANDS FOR SELECTED DATASETS ON HOST %s\n" % ', '.join(self._config.hosts)
        return self._create_zfs_destroy_script(predicate, header_line, report, writer)

# end class ZLimitsReportGenerator
