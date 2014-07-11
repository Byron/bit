#-*-coding:utf-8-*-
"""
@package zfs.sql.reports.duplication
@brief A report to show information about duplication of filesystems

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZDuplicationReportGenerator']

from datetime import (datetime,
                      timedelta)
from time import time
import os

import bapp
from bkvstore import StringList

from butility import (size_to_int,
                      int_to_size_string)
from bit.utility import (delta_to_tty_string,
                         datetime_days_ago,
                         delta_to_seconds,
                         float_to_tty_string)
from .base import ZReportGenerator
from .. import (ZPool,
                ZDataset,)
from zfs.snapshot import SnapshotSender


class ZDuplicationReportGenerator(ZReportGenerator, bapp.plugin_type()):
    """Find pools and or datasets that are above a certain metric limit"""

    type_name = 'duplication'
    description = "Find duplicates of filesystems based on the filesystem's name, which is used as primary identity \
                   of all clones, and present them as a simple tree with clones ordered by equivalence."
    _schema = ZReportGenerator._make_schema(type_name, dict(min_copies = 0,
                                                            min_equivalence = 0.0,
                                                            hosts = StringList,
                                                            name_like = '%',
                                                            ignore_filesystems_smaller = '0K',
                                                            ))

    report_schema = (       ('seen', timedelta, delta_to_tty_string),
                            ('    ', str, str),     # to draw some ascii art to indicate a tree
                            ('url', str, lambda o: o.url()),
                            ('copies', int, int),   # amount of copies
                            ('equivalence', float, lambda v: '%.2f%%' % v),
                            ('common_ss', str, lambda s: s and s.snapshot_name() or '-'),
                            ('used', int, int_to_size_string),
                            ('ratio', float, float_to_tty_string),
                            ('logical_used', int, int_to_size_string),
                            )

    # -------------------------
    ## @name Utilities
    # @{

    def _compute_equivalence(self, master_snapshots, master_snapshot_names, shadow):
        """@return tuple of the equivalence as float value from 0 to 1, 1 being the same, 0 being totally different, 
        and the latest common snapshot instance in the master filesystem. Snapshot can be None if there is 
        no common one
        @param master_snapshots return value of master.snapshots()
        @param master_snapshot_names plain names of master_snapshots, in order_by_desc
        @param shadow dataset instance of the shadow
        """
        index = -1
        for ss in reversed(list(shadow.snapshots())):
            try:
                index = master_snapshot_names.index(ss.snapshot_name())
                break
            except ValueError:
                pass
            # end handle index
        # end find matching snapshot

        # this can happen if the basename is similar, yet there are no snapshots or they are unrelated
        # Currently the 'basename-is-identity' convention isn't upheld everywhere
        if index < 0:
            return 0.0, None
        # end no match

        ss = master_snapshots[index]
        if index == len(master_snapshots) - 1:
            return 1.0, ss
        # end perfect match

        # Compute the relative amount of time that is not represented in the shadow
        total_time = delta_to_seconds(master_snapshots[-1].creation - master_snapshots[0].creation)
        missing_time = delta_to_seconds(master_snapshots[-1].creation - master_snapshots[index].creation)
        return (total_time - missing_time) / float(total_time), ss
    
    ## -- End Utilities -- @}

    def generate(self):
        # Create an initial query and map filesystems by basename
        rep = self.ReportType(self.report_schema)
        now = datetime.now()
        config = self.settings_value()
        query = self._session.query(ZDataset).filter(ZDataset.type == self.TYPE_FILESYSTEM).\
                                              filter(ZDataset.name.like(config.name_like)).\
                                              order_by(ZDataset.name, ZDataset.creation)

        bmap = dict()
        for inst in query:
            # Ignore pools and non-leaf filesystems
            if inst.is_pool_filesystem() or list(inst.children()):
                continue
            # end ignore non-leafs
            bmap.setdefault(os.path.basename(inst.name), list()).append(inst)
        # end build lookup


        min_copies, hosts, min_equivalence, ignore_size =   config.min_copies,\
                                                            config.hosts,\
                                                            config.min_equivalence,\
                                                            size_to_int(config.ignore_filesystems_smaller)
        radd = lambda r: rep.records.append(r) or r

        # Sort the list by num-copies-descending
        slist = sorted(bmap.items(), key=lambda t: len(t[1]), reverse=True)

        # Then compute the equivalence and build the report
        for basename, filesystems in slist:
            # For now, the oldest one is the master, which owns the copies
            # The host filter is applied here
            master = filesystems.pop(0)
            if min_copies and len(filesystems) + 1 >= min_copies or\
               hosts and master.host not in hosts or\
               master.used < ignore_size:
                continue
            # end skip if we are below threshold

            mrec = radd([now - master.updated_at,
                          (not len(filesystems) and '+' or '+-'),
                          master,
                          len(filesystems) + 1,
                          0.0,      # done later as avg
                          None,     # never set
                          master.used,
                          master.ratio,
                          master.used * master.ratio])

            crecs = list()
            msnapshots = list(master.snapshots())
            msnapshot_names = [ss.snapshot_name() for ss in msnapshots]
            for shadow in filesystems:
                equivalence, ss = self._compute_equivalence(msnapshots, msnapshot_names, shadow)
                crecs.append(radd([now - shadow.updated_at,
                                  '  `-',
                                  shadow,
                                  0,
                                  equivalence * 100.0,
                                  ss,
                                  shadow.used,
                                  shadow.ratio,
                                  shadow.used * shadow.ratio,
                            ]))
            # end for each copy

            if filesystems:
                mrec[4] = sum(r[4] for r in crecs) / len(crecs)
            # end compute average equivalence

            # drop previous records if equivalence is good enough
            if min_equivalence and mrec[4] >= min_equivalence:
                rep.records = rep.records[:-len(crecs) - 1]
        # end handle    

        return rep

    def generate_fix_script(self, report, writer):
        rid = 0
        recs = report.records
        lrecs = len(recs)
        last_host = None
        while rid < lrecs:
            rec = recs[rid]
            master = rec[2]
            ncopies = rec[3]
            equivalence = rec[4]
            next_rid = rid + ncopies
            shadow_recs = recs[rid+1:next_rid]
            rid = next_rid

            # can't fix those that don't have a shadow or that are complete
            if ncopies < 2 or equivalence >= 100.0:
                continue
            # end early bailout

            for srec in shadow_recs:
                shadow = srec[2]
                shadow_ss = srec[5]
                if shadow_ss is None:
                    writer("# Shadow filesystem at '%s' doesn't have a single snapshot in common with '%s' - cannot sync snapshots\n" % (shadow.url(), master.url()))
                    continue
                # end handle error
                if last_host is not None and last_host != master.host:
                    writer("# Host changed from '%s' to '%s' - please use hosts=<name> filters to generate one script per host\n" % (last_host, master.host))
                    return
                # end assure we stay on the same host
                last_host = master.host
                sss = SnapshotSender.new(master, shadow.url().parent_filesystem_url())
                sss.stream_script(writer)
        # end while stepping through report
