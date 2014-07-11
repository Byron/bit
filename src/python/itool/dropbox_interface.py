#-*-coding:utf-8-*-
"""
@package itool.dropbox_interface
@brief A commandline interface for handling the dropobx

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DropboxInterfaceSubCommand']

import sys
from pwd import getpwuid
from datetime import datetime


from .base import IToolSubCommand

import bapp
from bit.cmd import OverridableSubCommandMixin
from bit.reports import Report
from bit.utility import (  utc_datetime_to_date_time_string,
                           float_percent_to_tty_string,
                           IDParser,
                           none_support)

from butility import (login_name,
                      int_to_size_string)

from fsmonitor.daemon import DaemonThread
from fsmonitor.sql import (PackageSession,
                           SQLPackageTransaction,
                           SQLPackage)



class DropboxInterfaceSubCommand(IToolSubCommand, OverridableSubCommandMixin, bapp.plugin_type()):
    """Implements a commandline interface for handling dropbox transactions"""
    __slots__ = ()

    name = 'dropbox'
    description = "Interact with the dropbox system"
    version = '0.2.0'

    # -------------------------
    ## @name Constants
    # @{

    ARG_SUBCOMMAND = 'sub-commands'
    ARG_TRANSACTION_IDS = 'transaction-ids'


    ORDER_ASC = 'ascending'
    ORDER_DESC = 'descending'
    valid_orders = (ORDER_ASC, ORDER_DESC)

    ## Mode for listing transactions
    MODE_LIST = 'list'
    MODE_TRANSACTION = 'transaction'

    valid_modes = (MODE_LIST, MODE_TRANSACTION)

    ## Valid types for listing
    TYPE_TRANSACTION = 'transaction'
    TYPE_PACKAGE = 'package'

    valid_types = (TYPE_TRANSACTION, TYPE_PACKAGE)

    ## Actions to be applied to transactions
    ACTION_APPROVE = SQLPackageTransaction.ACTION_APPROVE
    ACTION_REJECT = SQLPackageTransaction.ACTION_REJECT
    ACTION_CANCEL = SQLPackageTransaction.ACTION_CANCEL
    ACTION_LIST_FILES = SQLPackageTransaction.ACTION_LIST_FILES

    valid_actions = (ACTION_APPROVE, ACTION_REJECT, ACTION_CANCEL, ACTION_LIST_FILES)
    query_actions = (ACTION_LIST_FILES,)

    ## Default limit for queries
    LIMIT_DEFAULT = 1000


    ## List everything, no filter
    FILTER_ANY = SQLPackage.FILTER_ANY
    FILTER_MANAGED = SQLPackage.FILTER_MANAGED
    FILTER_UNMANAGED = SQLPackage.FILTER_UNMANAGED
    FILTER_PENDING = SQLPackageTransaction.FILTER_PENDING
    FILTER_QUEUED = SQLPackageTransaction.FILTER_QUEUED
    FILTER_FINISHED = SQLPackageTransaction.FILTER_FINISHED
    FILTER_FAILED = SQLPackageTransaction.FILTER_FAILED
    FILTER_CANCELED = SQLPackageTransaction.FILTER_CANCELED
    FILTER_REJECTED = SQLPackageTransaction.FILTER_REJECTED

    valid_package_filters = (FILTER_MANAGED, FILTER_UNMANAGED, FILTER_ANY)
    valid_transaction_filters = (FILTER_PENDING, FILTER_QUEUED, FILTER_FINISHED, FILTER_FAILED,
                                 FILTER_CANCELED, FILTER_REJECTED, FILTER_ANY)

    SQL_FILTER_MAP = SQLPackage.FILTER_MAP
    SQL_FILTER_MAP.update(SQLPackageTransaction.FILTER_MAP)

    ## -- End Constants -- @}

    # -------------------------
    ## @name Configuration
    # @{

    report_schema_package = (   ('id', int, str),
                                ('root_path', str, str),
                                ('package_path', str, str),
                                ('managed_at', datetime, utc_datetime_to_date_time_string),
                                ('unmanaged_at', datetime, none_support(utc_datetime_to_date_time_string)),
                                ('stable_since', datetime, none_support(utc_datetime_to_date_time_string)),
                                ('comment', str, none_support(str)))


    report_schema_transaction = (   ('id', int, str),
                                    ('type_name', str, str),
                                    ('in_package', str, lambda p: p.root()),
                                    ('in_package_id', str, str),
                                    ('out_package', str, none_support(lambda p: p.root())),
                                    ('approved_by_login', str, lambda s: s == SQLPackageTransaction.TO_BE_APPROVED_MARKER and "NEEDS APPROVAL" or s),
                                    ('percent_done', str, lambda p: p is None and '-' or float_percent_to_tty_string(p)),
                                    ('spooled_at', datetime, none_support(utc_datetime_to_date_time_string)),
                                    ('started_at', datetime, none_support(utc_datetime_to_date_time_string)),
                                    ('finished_at', datetime, none_support(utc_datetime_to_date_time_string)),
                                    ('error', str, none_support(str)),
                                    ('comment', str, none_support(str)),
                                    ('reason', str, none_support(str)),)

    report_schema_files =                             (   ('path', str, str),
                                                          ('size', int, int_to_size_string),
                                                          ('uid', str, str), 
                                                          ('gid', int, str),
                                                          ('mode', int, lambda m: "%o" % m))

    SQL_TYPE_MAP = { TYPE_TRANSACTION : (SQLPackageTransaction, report_schema_transaction, 'transaction_filter'),
                     TYPE_PACKAGE : (SQLPackage, report_schema_package, 'package_filter') }
    
    ## -- End Configuration -- @}



    # -------------------------
    ## @name Subcommand Implementation
    # @{

    def _handle_list(self, args, session):
        """Implement list subcommand"""
        SQLType, schema, filter_attr = self.SQL_TYPE_MAP.get(args.type)
        assert SQLType, "Unkown type encountered, updated SQL_TYPE_MAP"

        report = Report(columns=schema)
        query = session.query(SQLType)

        # Handle filtering
        ###################
        filter_name = getattr(args, filter_attr)
        if filter_name != self.FILTER_ANY:
            sql_filter = self.SQL_FILTER_MAP.get(filter_name)
            assert sql_filter is not None, "Unknown sql filter, update SQL_FILTER_MAP with '%s'" % filter_name
            query = query.filter(sql_filter)
        # end handle pre-packaged filter

        # sort it
        query = query.order_by(getattr(SQLType.id, args.order == self.ORDER_DESC and 'desc' or 'asc')())

        limit = args.limit is None and self.LIMIT_DEFAULT or args.limit
        if limit > 0:
            query = query.limit(limit)
        # end finally, set limit

        # Convert data to report
        ########################
        record = report.records.append
        count = -1
        for count, item in enumerate(query):
            record(tuple(getattr(item, vals[0]) for vals in schema))
        # end for each item

        # And output it
        report.serialize(args.output_mode, sys.stdout.write, column_names=not args.no_header)

        if count == -1:
            sys.stderr.write("No '%s' record found\n" % args.type)
        if count + 1 == limit:
            sys.stderr.write("WARNING: Output was limited to %i - see the --limit flag\n" % args.limit)
        # end handle warnings and errors

        return self.SUCCESS

    def _handle_transactions(self, args, session):
        """Implement transaction subcommand"""
        if args.action in self.query_actions:
            if args.reason:
                self.log().warn("--reason has no effect in query actions like %s", ', '.join(self.query_actions))
            # end handle reason
        else:
            if not args.reason:
                self.log().error("Please specify a reason for performing the '%s' action, use the --reason argument", args.action)
                return self.ERROR
            # end need reason
        # end assure reason is set

        try:
            for tid in getattr(args, self.ARG_TRANSACTION_IDS):
                trans = session.query(SQLPackageTransaction).filter(SQLPackageTransaction.id == tid)[:]
                if not trans:
                    raise ValueError("No transaction found with id %i" % tid)
                # end fail on missing transactions
                trans = trans[0]
                if args.action == self.ACTION_APPROVE:
                    if trans.finished_at is not None or trans.started_at is not None:
                        raise ValueError("Transaction %i is already done and cannot be approved after the fact" % tid)
                    # end handle finished transactions
                    trans.approved_by_login = login_name()
                    self.log().info("Approved %i" % tid)
                elif args.action == self.ACTION_REJECT:
                    trans.reject_approval()
                    self.log().info("Rejected %i" % tid)
                elif args.action == self.ACTION_CANCEL:
                    trans.cancel()
                    self.log().info("Canceled %i" % tid)
                elif args.action == self.ACTION_LIST_FILES:
                    print "Files for transaction %i" % tid
                    report = Report(columns=self.report_schema_files)
                    record = report.records.append
                    for f in trans.files:
                        try:
                            uid = getpwuid(f.uid).pw_name
                        except KeyError:
                            uid = f.uid
                        # end remap uid if possible
                        record((f.path, f.size, uid, f.gid, f.mode))
                    # end for each file
                    report.serialize(Report.SERIALIZE_TTY, sys.stdout.write)
                else:
                    raise NotImplemented("unknown action: %s" % args.action)
                # end handle action

                # Always keep the reason around
                if args.action not in self.query_actions:
                    assert args.reason
                    trans.reason = args.reason
                # end assure reason is set only in edit mode
            # end for each tid
            session.commit()
        except Exception, err:
            self.log().error(str(err))
            if args.action not in self.query_actions:
                session.rollback()
                self.log().error("Failed to set transaction - all progress rolled back")
            # end don't warn if we are read-only
            return self.ERROR
        # end handle exception
        return self.SUCCESS
    
    ## -- End Subcommand Implementation -- @}

    def setup_argparser(self, parser):
        super(DropboxInterfaceSubCommand, self).setup_argparser(parser)

        help = "Dropbox subcommands"
        subparser_commands = parser.add_subparsers(dest=self.ARG_SUBCOMMAND, help=help)

        ## Sub-command 'list'
        ######################
        help = "Display information about transactions and packages"
        parser_list = subparser_commands.add_parser(self.MODE_LIST, help=help)
        parser_list.add_argument('type', choices=self.valid_types, help=help)

        default_filter = self.FILTER_MANAGED
        help = "Filters packages by the given state, default is '%s';" % default_filter
        help += "%s = packages which are currently seen by the daemon;" % self.FILTER_MANAGED
        help += "%s = packages which are not seen anymore (i.e. deleted, moved);" % self.FILTER_UNMANAGED
        help += "%s = list all entries in the database" % self.FILTER_ANY
        parser_list.add_argument('-pf', '--package-filter', dest='package_filter', default=default_filter,
                                 choices=self.valid_package_filters,
                                 help=help)

        default_filter = self.FILTER_PENDING
        help = "Filters transactions by the given state, default is '%s';" % default_filter
        help += "%s = awaiting approval;" % self.FILTER_PENDING
        help += "%s = currently queued or in progress;" % self.FILTER_QUEUED
        help += "%s = it was processed without error;" % self.FILTER_FINISHED
        help += "%s = it was processed with error;" % self.FILTER_FAILED
        help += "%s = canceled by user;" % self.FILTER_CANCELED
        help += "%s = rejected by user;" % self.FILTER_REJECTED
        help += "%s = list all entries in the database" % self.FILTER_ANY
        parser_list.add_argument('-tf', '--transaction-filter', dest='transaction_filter', 
                                 default=default_filter,
                                 choices=self.valid_transaction_filters,
                                 help=help)

        help = "Restrict amount of returned packages to given number. Set it to 0 if no limit is desired. Default: %(default)s"
        parser_list.add_argument('--limit', dest='limit', default=self.LIMIT_DEFAULT, type=int, help=help)

        help = "Specifies the way results are presented to the user, either in human-readable form, or as CSV"
        parser_list.add_argument('-o', '--output-mode', dest='output_mode', default=Report.SERIALIZE_TTY,
                                 choices=(Report.SERIALIZE_TTY, Report.SERIALIZE_CSV),
                                 help=help)

        help = "If set, column names will not be printed as first line. Useful for scripting"
        parser_list.add_argument('--skip-header', dest='no_header', default=False, action='store_true',
                                 help=help)

        help = "Specify a sort order, default is %(default)s"
        parser_list.add_argument('--sort-order', dest='order', default=self.ORDER_DESC,
                                 choices=self.valid_orders,
                                 help=help)

        OverridableSubCommandMixin.setup_argparser(self, parser_list)

        ## Sub-command 'transaction'
        ############################
        help = "Change existing transactions"
        parser_transaction = subparser_commands.add_parser(self.MODE_TRANSACTION, help=help)
        
        help = "The action to perform on transactions, actions are:"
        help += "%s = approve a transaction that need approval;" % self.ACTION_APPROVE
        help += "%s = reject a transaction, it's package will NEVER be synced again unless re-created or renamed;" % self.ACTION_REJECT
        help += "%s = cancel a transaction - a new transaction will be spawned when its package changes;" % self.ACTION_CANCEL
        help += "%s = list all files touched by a transaction;" % self.ACTION_LIST_FILES
        parser_transaction.add_argument('action', choices=self.valid_actions, help=help)

        help = "One or more transaction id as integer to know the transactions to operate on"
        parser_transaction.add_argument(self.ARG_TRANSACTION_IDS, nargs='+', type=int, help=help)

        help = "A reason in case you want to reject a transaction - in the latter case it is mandatory"
        parser_transaction.add_argument('-r', '--reason', dest='reason', type=str, help=help)

        OverridableSubCommandMixin.setup_argparser(self, parser_transaction)

        return self

    def execute(self, args, remaining_args):
        self.apply_overrides(DaemonThread.schema(), args.overrides)
        subcmd = getattr(args, self.ARG_SUBCOMMAND)
        config = DaemonThread.settings_value()

        # Assure the caller may actually call us
        res = IDParser().parse(login_name())
        if config.authentication.privileged_group not in [g[1] for g in res.groups]:
            self.log().error("Your are not authorized to run this program - user '%s' is not in group '%s'" %
                                                                        (login_name(), config.authentication.privileged_group))
            return 255
        #end check authentication

        session = PackageSession.new(url=config.db.url)

        if subcmd == self.MODE_LIST:
            return self._handle_list(args, session)
        elif subcmd == self.MODE_TRANSACTION:
            return self._handle_transactions(args, session)
        else:
            raise NotImplemented("Subcommand '%s' unknown" % subcmd)
        # end handle subcommands


# end class DropboxInterfaceSubCommand
