#-*-coding:utf-8-*-
"""
@package zfs.sql.reports.reserve
@brief A report to compute zfs reserve configuration for configured filesystems

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZReserveReportGenerator']

from datetime import (datetime,
                      timedelta)
from time import time
import copy
import os

from bkvstore import StringList


from bit.utility import (size_to_int,
                         delta_to_tty_string,
                         int_to_size_string,
                         float_percent_to_tty_string,
                         DistinctStringReducer,
                         ravg,
                         rsum)
from zfs.url import ZFSURL
from .base import (ZReportGenerator,
                   host_filter)
from .. import ZDataset


class ZReserveReportGenerator(ZReportGenerator, Plugin):
    """Find filesytems marked with a priority attribute and compute a filesystem reserve value to distribute
    free space among all filesystems based on that value."""

    type_name = 'reserve'
    description = "Compute the correct quota or reserve values for all filesystems which have the ``zfs:priority`` property set."
    _schema = ZReportGenerator._make_schema(type_name, dict(max_cap = 80.0,
                                                            # total space we should distribute among the filesystems
                                                            distribute_space = str,
                                                            hosts = StringList,
                                                            # A list of priorities, one for each filesystem
                                                            debug_priorities=list,
                                                            pool_name = '%',
                                                            # mode of operation, either change quota or reserve
                                                            mode='quota'
                                                            ))

    report_schema = (       ['seen', timedelta, delta_to_tty_string, ravg],
                            ['url', str, lambda o: isinstance(o, str) and o or o and o.url(), DistinctStringReducer()],
                            ['priority', int, int, rsum],
                            ['used', int, int_to_size_string, rsum],
                            ['reserved', int, int_to_size_string, rsum],
                            ['remaining', int, int_to_size_string, rsum],
                            ['change', int, int_to_size_string, rsum],
                            ['%full', int, float_percent_to_tty_string, ravg],
                            )

    # -------------------------
    ## @name Constants
    # @{

    MODE_RESERVATION = 'reservation'
    valid_modes = ('quota', MODE_RESERVATION)
    
    ## -- End Constants -- @}

    def generate(self):
        # Create an initial query and map filesystems by basename
        rep = self.ReportType(copy.deepcopy(self.report_schema))
        now = datetime.now()
        config = self.settings_value()
        if config.mode not in self.valid_modes:
            raise ValueError("Can only support the following modes: %s" % ', '.join(self.valid_modes))
        # end handle

        rep.columns[4][0] = config.mode == self.MODE_RESERVATION and 'reserved' or 'quota'

        query = self._session.query(ZDataset).filter(ZDataset.avail != None).\
                                              filter(ZDataset.zfs_priority != None).\
                                              filter(ZDataset.name.like(config.pool_name + '%/%'))

        query = host_filter(config.hosts, ZDataset.host, query)
        fs_map = dict()
        for fs in query:
            if fs.property_is_inherited('zfs_priority'):
                continue
            fs_map.setdefault((fs.host, fs.url().pool()), list()).append(fs)
        # end for each filesystem

        distribute_space = config.distribute_space
        if distribute_space:
            distribute_space = size_to_int(distribute_space)
        # end convert space

        if distribute_space and config.max_cap:
            raise ValueError("Please specify either 'max_cap or 'distribute_space', or set one of them to 0")
        # end assure we don't see both

        if config.debug_priorities and len(fs_map) > 1:
            raise AssertionError("If debug_priorities are used, you muse limit the amount of involved hosts to one")
        # end make sure debugging makes sense

        for (host, pool), fs_list in fs_map.iteritems():
            if config.debug_priorities and len(config.debug_priorities) != len(fs_list):
                raise AssertionError("Please specify exactly %i priorities, one for each filesystem, got %i" % (len(fs_list), len(config.debug_priorities)))
            # end verify priorities

            priorities = config.debug_priorities or [fs.zfs_priority for fs in fs_list]
            total_parts = sum(priorities)
            pool = self._session.instance_by_url(ZFSURL.new(host, pool))
            if distribute_space:
                total_alloc = distribute_space
            else:
                total_alloc = pool.size * (config.max_cap / 100.0)
            # end handle total_alloc

            for fs, prio in zip(fs_list, priorities):
                reserve = (total_alloc / float(total_parts)) * prio
                rep.records.append([now - fs.updated_at,
                                    fs,
                                    prio,
                                    fs.used,
                                    reserve,
                                    reserve - fs.used,
                                    reserve - fs.avail,
                                    (fs.used / float(reserve)) * 100.0
                                    ])
            # end for each filesystem
        # end for each pool-host pair
        if len(rep.records) > 1:
            rep.records.append(rep.aggregate_record())
        # end aggregate only if it makes sense

        return rep

    def generate_fix_script(self, report, writer):
        last_host = None
        mode = self.settings_value().mode

        for rec in report.records:
            fs, reserve = rec[1], rec[4]
            if isinstance(fs, str):
                continue
            # end skip aggregation records - filesystem is a string in that case
            if last_host and last_host != fs.host:
                writer('# Cannot proceed as the host changed during iteration - please re-run the report with the hosts=name value set\n')
                break
            # end verify we stay on a single host
            if last_host is None:
                writer("# Reservation automation for host '%s'\n" % fs.host)
            # end initial info
            last_host = fs.host
            if reserve < fs.used:
                writer("# Reserve for '%s' is already to low (%s reserved vs %s used), consider increasing its zfs:priority\n" % (fs.url(),
                                                                                                                                 int_to_size_string(reserve), 
                                                                                                                                 int_to_size_string(fs.used)))
            else:
                writer("zfs set %s=%s %s\n" % (mode, int_to_size_string(reserve), fs.name))
            # end handle reserve issue
        # end for each record
        return True


# end ZReserveReportGenerator
