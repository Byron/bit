#-*-coding:utf-8-*-
"""
@package zfs.sql.reports.base
@brief Base implementations and utilities

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZReportGenerator', 'host_filter']

from butility import (LazyMixin,
                      int_to_size_string,
                      size_to_int)

from bit.utility import (delta_to_tty_string,
                         float_percent_to_tty_string,
                         datetime_days_ago,
                         datetime_to_date_string)
from bit.reports import ReportGenerator
from .. import ZSession

from datetime import (datetime,
                      timedelta)


# -------------------------
## @name Utilities
# Shared utilities for report generators
# @{

def host_filter(hosts, hosts_attribute, query):
    """Given an ORM attribute and ORM query, add a filter to only operate on the given hosts.
    @param hosts a list of hosts by which to filter. If empty, all hosts will be allowed (equal to no host filtering)
    @param hosts_attribute an orm attribute for hosts, like ZDataset.host or ZPool.host
    @param query the query to alter"""
    if hosts:
        query = query.filter(hosts_attribute.in_(hosts))
    # end adjust filter
    return query
# end handle filter

## -- End Utilities -- @}


class ZReportGenerator(LazyMixin, ReportGenerator):
    """Base class for all reports, using the SQL database as data source"""
    __slots__ = (
                    # Session to query for data
                    '_session',
                    # configuration data
                    '_config'
                )

    # -------------------------
    ## @name Constants
    # For use in respective report fields
    # @{

    ## Types for use in the report schema field : type
    TYPE_POOL = 'pool'
    TYPE_SNAPSHOT = 'snapshot'
    TYPE_FILESYSTEM = 'filesystem'
    TYPE_AGGREGATE = 'aggregate'
    TYPE_SUMMARY = 'summary'

    # Allow us to put in strings as well
    delta_to_string = lambda x: isinstance(x, str) and x or delta_to_tty_string(x)
    datetime_to_date = lambda x: isinstance(x, str) and x or datetime_to_date_string(x)

    report_schema = (       ('seen', timedelta, delta_to_string),
                            ('host', str, str),
                            ('name', str, str),
                            ('type', str, str),
                            ('ctime', timedelta, datetime_to_date), 
                            ('cdtime', timedelta, delta_to_string),
                            ('size_B', int, int_to_size_string),
                            ('free_B', int, int_to_size_string),
                            ('cap', float, float_percent_to_tty_string),
                            ('comment', str, str),)

    REPORT_ROOT_KEY = ZSession.settings_schema().key().split('.')[0] + '.report'

    ## -- End Constants -- @}


    def __init__(self, args, session = None, data = None):
        """Initialize this instance"""
        super(ZReportGenerator, self).__init__(args)
        
        if session:
            assert isinstance(session, ZSession)
            self._session = session
        if data:
            self._config = data
        # end initialize data

    def _set_cache_(self, name):
        if name == '_config':
            self._config = self.settings_value()
        elif name == '_session':
            self._session = ZSession.new()
        else:
            super(ZReportGenerator, self)._set_cache_(name)
        # end handle cache name
    # -------------------------
    ## @name Subclass Utilities
    # @{

    @classmethod
    def _obj_to_record(cls, obj, columns):
        """@return a list of values retrieved from the given sqlalchemy object, based on the given list of column names"""
        return [getattr(obj, n) for n in columns]

    @classmethod
    def _aggregate_records(cls, records, now):
        """Append a record representing the default aggregation of the given report, to the report
        @param records as generated by generate() matching our report_schema, e.g. report.records
        @param now datetime object representing your 'now'
        @note the aggregated result will on be added if records is not empty
        """
        if records:
            agg = [now - now, 'NA', 'SUMMARY', cls.TYPE_AGGREGATE, now, now-now, 0, 0, 0.0,
                    '#records: %i, avg(seen) sum(size_b) sum(free_b) avg(cap)' % len(records)]
            nr = len(records)
            hosts = set()
            for rec in records:
                hosts.add(rec[1])
                agg[0] += rec[0]
                agg[6] += rec[6]
                agg[7] += rec[7]
                agg[8] += rec[8]
            # end for each record

            agg[0] /= nr
            agg[1] = '#%i' % len(hosts)
            agg[8] /= nr

            records.append(agg)
        # end aggregate only if there are records

    @classmethod
    def _create_zfs_destroy_script(cls, predicate, header_line, report, writer):
        """Generate a script that destroys datasets in the give report if the predicate matches
        @param predicate f(record) -> Bool returns True for each record that should be considered for destruction
        @param header_line a line to be printed before the destruction line
        @param report as generated by generate()
        @param writer a function to write given bytes
        @return True to indicate a script was created"""
        # filter records ...
        valid_records = list()
        for rid, rec in enumerate(report.records):
            if predicate(rec):
                valid_records.append(rec)
            # end 
        # end for each record (prefilter)

        last_host = ''
        lr = len(valid_records)

        for rid, rec in enumerate(valid_records):
            host = rec[1]
            name = rec[2]
            comment = rec[-1]

            if last_host:
                if last_host != host:
                    writer('# ABORTED SCRIPT GENERATION ON RECORD %i OF %i AS THE HOST CHANGED. USE THE -s hosts=<name> FILTER TO CONTROL FOR WHICH HOST TO GENERATE THE SCRIPTS\n' % (rid, len(report.records)))
                    break
                # end abort on host change
            else:
                last_host = host
                writer('echo "%s"\n' % header_line)
            # end setup host check

            writer('echo `date` - %i%%: Destroying dataset %i of %i\n' % ((rid/float(lr)) * 100, rid + 1, lr))
            cmd = 'zfs destroy %s' % name
            writer('echo "%s  (%s)"\n' % (cmd, comment))
            writer(cmd + ' || exit $?\n')
        # end for each rec
        else:
            writer("# No candidate datasets found in report for zfs destroy")
        # end handle empty loop

        return True

    
    ## -- End Subclass Utilities -- @}

    # -------------------------
    ## @name Interface
    # @{

    def configuration(self):
        """@return a dictionary with all our custom configured values"""
        return self._config

    def generate_fix_script(self, report, writer):
        writer("# Cannot generate fix-script for report of type '%s'" % self.type_name)
        return False
        
    ## -- End Interface -- @}

    

# end class ZReportGenerator
