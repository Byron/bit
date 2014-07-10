#-*-coding:utf-8-*-
"""
@package zfs.cmd.filesystem
@brief A command for dealing with filesystems

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['FilesystemSubCommand']

import sys

from .base import ZFSSubCommand

from zfs.snapshot import SnapshotSender
from zfs.sql import ZSession
from zfs.url import ZFSURL

from bcmd.argparse import ParserError
from bit.utility import DistinctStringReducer


class FilesystemSubCommand(ZFSSubCommand, Plugin):
    """Use reports (as plugins) whose interface is made available through the commandline"""
    __slots__ = ()

    name = 'filesystem'
    description = 'Handle filesystems in various ways'
    version = '0.1.0'

    allow_unknown_args = True

    # -------------------------
    ## @name Constants
    # @{

    MODE_SYNC = 'sync'
    MULTI_SYNC_MODE = 'configured'
    modes = (MODE_SYNC,)

    ## -- End Constants -- @}

    def setup_argparser(self, parser):
        super(FilesystemSubCommand, self).setup_argparser(parser)

        help = "The mode of operation."
        help += "%s: synchronize filesystem of source to destination. The latter can be either a zfs url or one of %s" % (self.MODE_SYNC, ', '.join(SnapshotSender.dest_fs_modes))
        parser.add_argument('mode', choices=self.modes, help=help)

        help = "If set, a report will be generated with all possible destinations that could hold the source filesystem"
        parser.add_argument('-l', '--list-all-targets', action='store_true', dest='report', help=help)

        help = "If set, a script will be generated that would transfer the source to destination"
        parser.add_argument('--script', action='store_true', dest='script', help=help)

        return self

    def execute(self, args, remaining_args):
        def handle_report(report):
            if not report.records:
                self.log().info('No result')
            else:
                report.serialize(report.SERIALIZE_TTY, sys.stdout.write)
            # end handle empty reports
        # end utility

        if args.mode == self.MODE_SYNC:
            if len(remaining_args) == 1:
                remaining_args.append(SnapshotSender.DEST_MODE_AUTO)
            # end auto-setup

            if len(remaining_args) != 2:
                raise ParserError("Please specify source and destination file-system zfs url, i.e. sync zfs://host/fs zfs://host/dest-fs, destination can be left out for auto-mode.\nCan also be auto, property, configured")
            # end verify arguments

            surl, durl = remaining_args
            surl = ZFSURL(surl)
            session = ZSession.new()

            try:
                if args.report:
                    handle_report(SnapshotSender.report_candidates(session.instance_by_url(surl)))
                    return self.SUCCESS
                # end handle complete report

                sfs = session.instance_by_url(surl, force_dataset=True)
                if durl == self.MULTI_SYNC_MODE:
                    senders = SnapshotSender.new_from_properties(sfs)
                else:
                    senders = [SnapshotSender.new(sfs, durl)]
            except ValueError, err:
                self.log().error(str(err))
                return self.ERROR
            # end handle invalid source

            if not senders:
                self.log().info('no filesystem configured a destination url using the zfs:receive-url property')
                return self.SUCCESS
            # end handle no result

            if args.script:
                for ss in senders:
                    ss.stream_script(sys.stdout.write)
                # end for each sender to write a script for
            else:
                # by default we generate a report
                rep = senders[0].report()
                for ss  in senders[1:]:
                    ss.report(rep)
                # end for each report to generate
                if len(rep.records) > 1:
                    agr = rep.aggregate_record()
                    rep.records.append(agr)
                # end aggregation makes sense only if there are multiple records
                handle_report(rep)
        # end handle mode

        return self.SUCCESS

# end class FilesystemSubCommand
