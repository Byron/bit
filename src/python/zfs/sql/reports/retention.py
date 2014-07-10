#-*-coding:utf-8-*-
"""
@package zfs.sql.reports.retention
@brief A report for listing all snapshots that do not fit into the respective retention policy

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZRetentionReportGenerator', 'RetentionPolicy']

from datetime import datetime
from time import time
import logging

from bkvstore import StringList

from bit.retention import RetentionPolicy
from bit.utility import   (seconds_to_datetime,
                           delta_to_tty_string)
from .base import (ZReportGenerator,
                   host_filter)
from .. import ZDataset


log = logging.getLogger('zfs.sql.reports.retention')



class ZRetentionReportGenerator(ZReportGenerator, Plugin):
    """Find pools and or datasets that are above a certain metric limit"""
    __slots__ = ()

    type_name = 'retention'
    description = "A complex subcommand which shows all snapshots which would be deleted based on a particular \
                   retention policy. Defining this policy is easy once you have understood the system."
    _schema = ZReportGenerator._make_schema(type_name, dict(policy = str,
                                                            applied_every = str,
                                                            hosts = StringList,
                                                            debug = int,
                                                            name_like = str))

    PolicyType = RetentionPolicy

    def generate(self):
        now = datetime.now()
        now_time = time()
        rep = self.ReportType()
        rep.columns.extend(self.report_schema)

        policy_string = self._config.policy
        name_like = self._config.name_like
        applied_every_string = self._config.applied_every
        debug = self._config.debug


        if not policy_string:
            # todo find it from filesystem property
            log.error('Retention policy is not configured')
            return rep
        # end ignore empty retention

        if not name_like:
            log.error("Please specify the name_like to be the name of the file system, like '%foo%'")
            return rep
        # end handle name filter not set


        policy = self.PolicyType(policy_string)
        applied_every_string = applied_every_string or None

        # Find all snapshots ascending by creation date and 
        query = self._session.query(ZDataset).\
                                filter(ZDataset.avail == None).\
                                filter(ZDataset.name.like(self._config.name_like)).\
                                order_by(ZDataset.host, ZDataset.creation)
        # sort all results by filesystem
        by_fs_map = dict()
        for ss in host_filter(self._config.hosts, ZDataset.host, query):
            by_fs_map.setdefault((ss.host, ss.filesystem_name()), list()).append((ss.creation, ss))
        # end for each dataset


        def count_samples_in_range(samples, from_date, to_date):
            count = 0
            for ctime, _ in samples:
                if from_date < ctime < to_date:
                    count += 1
                elif count:
                    break
                # end handle early bailout
            # end for each sample
            return count
        # end brute force count samples utility, doesn't make assumptions about order


        kept_comment = 'kept by policy'
        removed_comment = 'removed by policy'
        summaries = list()              # summary-records
        for (fs_host, fs_name), samples in by_fs_map.iteritems():
            # Apply policy and prepare actual report
            remaining, deleted = policy.filter(now_time, samples)

            # in debug mode, we want to see it even there are no deletions
            # Otherwise this is just a shortcut
            if not debug and not deleted:
                continue
            # end handle empty list

            if debug:
                merged_records = list()
                dset = set(deleted)
                for sample in samples:
                    is_deleted = sample in dset
                    ctime, ss = sample
                    rep.records.append([    now - ss.updated_at,
                                            fs_host,
                                            ss.name,
                                            is_deleted and self.TYPE_SNAPSHOT or 'debug',
                                            ss.creation,
                                            now - ss.creation,
                                            ss.used,
                                            0,
                                            100.0,
                                            is_deleted and removed_comment or kept_comment])
                # end for each sample

                # Convert rules into format that is more easily understood: num-samples:date-ago
                rule_tokens = list()
                total_duration = 0
                to_date = now
                for keep, freq, duration in policy._rules:
                    total_duration += duration
                    from_date = seconds_to_datetime(now_time - total_duration)
                    remaining_count = count_samples_in_range(remaining, from_date, to_date)
                    del_count = count_samples_in_range(deleted, from_date, to_date)
                    rule = '(%i-%i=%i)/%i:%s' % (remaining_count + del_count, del_count, remaining_count,
                                                duration / freq, 
                                                delta_to_tty_string(now - from_date))
                    rule_tokens.append(rule) 
                    to_date = from_date
                # end for each rule

                summaries.append([now-now,
                                  fs_host,
                                  fs_name,
                                  'debug-' + self.TYPE_SUMMARY,
                                  now,
                                  now - now,
                                  0,
                                  0,
                                  0,
                                  ','.join(rule_tokens)
                            ])

            # end adjust record source for debugging
            else:
                for creation_time, ss in deleted:
                    rep.records.append([    now - ss.updated_at,
                                            fs_host,
                                            ss.name,
                                            self.TYPE_SNAPSHOT,
                                            ss.creation,
                                            now - ss.creation,
                                            ss.used,
                                            0,
                                            100.0,
                                            removed_comment])
            # end handle debug

            summary = "%s - Removing %i of %i snapshots; %i remain, policy-max = %i (+%i)" % ((policy_string, len(deleted), 
                                                                                            len(samples), len(remaining))
                                                                                            + policy.num_rule_samples(applied_every_string))
                                                                                        
            summaries.append([now-now,
                              fs_host,
                              fs_name,
                              self.TYPE_SUMMARY,
                              now,
                              now - now,
                              0,
                              0,
                              0,
                              summary
                            ])

        # end for each filesystem

        # AGGREGATE
        ###########
        self._aggregate_records(rep.records, now)
        rep.records.extend(summaries)


        return rep

    def generate_fix_script(self, report, writer):
        predicate = lambda r: r[3] == self.TYPE_SNAPSHOT
        header_line = "DELETING SNAPSHOTS THAT DIDN'T PASS THE RETENTION TEST"
        return self._create_zfs_destroy_script(predicate, header_line, report, writer)
