#-*-coding:utf-8-*-
"""
@package zfs.cmd.list
@brief A command for listing and gently filtering zfs information. For now, just for human consumption

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ListSubCommand']

import sys
from datetime import (datetime,
                      timedelta)

from .base import ZFSSubCommand
from zfs.sql import (ZSession,
                     ZPool,
                     ZDataset)
from zfs.url import ZFSURL
from zfs.sql.reports import ZReportGenerator
from bit.reports import Report
import bapp
from bapp import ApplicationSettingsMixin
from bkvstore import KeyValueStoreSchema
from butility import int_to_size_string
from bit.utility import (delta_to_tty_string,
                         datetime_to_date_string,
                         float_percent_to_tty_string,
                         float_to_tty_string,
                         rsum,
                         ravg,
                         DistinctStringReducer)


class ListSubCommand(ZFSSubCommand, ApplicationSettingsMixin, bapp.plugin_type()):
    """Use reports (as plugins) whose interface is made available through the commandline"""
    __slots__ = ()

    name = 'list'
    description = 'Sort and display filesystems and pools'
    version = '0.1.0'

    # -------------------------
    ## @name Constants
    # @{

    COLUMN_ALL = 'all'
    COLUMN_IMPORTANT = 'important'
    COLUMN_URL = 'url'

    ORDER_ASC = 'asc'
    ORDER_DESC = 'desc'
    order = (ORDER_DESC, ORDER_ASC)

    TYPE_DATASET = 'dataset'
    types = (ZReportGenerator.TYPE_POOL, ZReportGenerator.TYPE_FILESYSTEM, ZReportGenerator.TYPE_SNAPSHOT, TYPE_DATASET)

    ## Maps column names to converter and reducer
    int_to_size = lambda x: x is None and '-' or int_to_size_string(x)

    schema_map = { 
                    'creation' : (timedelta, delta_to_tty_string, ravg),
                    'updated_at' : (timedelta, delta_to_tty_string, ravg),
                    'used' : (int, int_to_size_string, rsum),
                    'refer' : (int, int_to_size_string, rsum),
                    'size' : (int, int_to_size_string, rsum),
                    'free' : (int, int_to_size_string, rsum),
                    'alloc' : (int, int_to_size_string, rsum),
                    'avail' : (int, int_to_size, rsum),
                    'quota' : (int, int_to_size, rsum),
                    'usedds' : (int, int_to_size, rsum),
                    'usedchild' : (int, int_to_size, rsum),
                    'ratio' : (float, lambda f: '%.2fx' % f, ravg),
                    'cap' : (float, float_percent_to_tty_string, ravg),
                  }

    ## -- End Constants -- @}

    _schema = KeyValueStoreSchema('%s.list' % ZSession.settings_schema().key().split('.')[0], {
                    'columns' : 
                        { 'dataset' : [COLUMN_URL, 'type', 'creation', 'used', 'avail', 'refer',
                                       'ratio', 'quota', 'reserv', 'usedds', 'usedchild'],
                        'filesystem' : [COLUMN_URL, 'type', 'creation', 'used', 'avail', 'refer',
                                       'ratio', 'quota', 'reserv', 'usedds', 'usedchild'],
                        'snapshot' : [COLUMN_URL, 'creation', 'used', 'ratio'],
                        'pool' : ['host', 'name', 'size', 'cap', 'health', 'free', 'alloc']}
                })


    # -------------------------
    ## @name Utilities
    # @{

    def table_schema_from_colums(self, table_columns, column_names):
        """@return a schema suitable for a Table instance, based on the given table_columns and column_names
        @param column_names names of columns to take into consideration."""
        schema = list()
        none_to_dash = lambda x: x is None and '-' or x
        for name in column_names:
            if name != self.COLUMN_URL:
                try:
                    table_columns[name]
                except KeyError:
                    self.log().warn("Ignored unknown column name: '%s'", name)
                    continue
                # end handle unknown name
            # end ignore url column - there is no match in database

            if name not in self.schema_map:
                additional = [str, none_to_dash, DistinctStringReducer()]
            else:
                additional = self.schema_map[name]
            # end handle additional schema

            schema.append([name] + list(additional))
        # end for each column name to choose from 
        return schema
        
    def columns_by_names(self, columns, names):
        """@return a list of columns matching the given names. Will ignore names that don't match a column"""
        res = list()
        for name in names:
            if name not in columns:
                continue
            res.append(columns[name])
        # end for each name
        return res

    def verify_columns(self, columns, names):
        """@return the given input 'names' if all are valid columns, or None.
        @note errors will be printed on mismatch"""
        found_bad_col = False
        for col in names:
            if col != self.COLUMN_URL and col not in columns:
                self.log().error("Column '%s' does not exist in database", col)
                found_bad_col = True
            # end check bad
        # end for each column to check

        if found_bad_col:
            return None

        return names
        
    
    ## -- End Utilities -- @}


    def setup_argparser(self, parser):
        super(ListSubCommand, self).setup_argparser(parser)

        help = "The type of object you want to list, like 'pool' or 'filesystem'"
        parser.add_argument('type', choices=self.types, help=help)

        help = "The exact name of the host you want to look at."
        parser.add_argument('--host', dest='host', type=str, help=help)

        help = "Ignore all filesystems which have children"
        parser.add_argument('-lo', '--leaf-only', dest='leaf', action='store_true', 
                            default=False, help=help)

        help = "Names of columns you want to see. If not set, you will see all columns"
        help += "Can take the special value '%s' to display everything, and defaults to '%s' for the most important ones." % (self.COLUMN_ALL, self.COLUMN_IMPORTANT)
        parser.add_argument('-o', '--columns', dest='columns', type=str, metavar='COLUMN', default=[self.COLUMN_IMPORTANT],
                                    nargs='+', help=help)

        help = "Names of columns by which to sort in ascending order."
        parser.add_argument('-s', '--order-by-asc', dest='order_by_asc', type=str, metavar='COLUMN', nargs='+', help=help)

        help = "Names of columns by which to sort in descending order."
        parser.add_argument('-S', '--order-by-desc', dest='order_by_desc', type=str, metavar='COLUMN', nargs='+', help=help)

        help = "The name of the dataset or pool. It will be matched automatically using a substring search."
        parser.add_argument('-n', '--name', dest='name', type=str, help=help)

        help = "If set, only the aggregate line will be shown"
        parser.add_argument('-a', '--aggregate-only', dest='aggregate_only', action='store_true', default=False, help=help)

        return self

    def execute(self, args, remaining_args):
        config = self.settings_value()
        session = ZSession.new()
        zcls = args.type == ZReportGenerator.TYPE_POOL and ZPool or ZDataset
        query = session.query(zcls)
        table = zcls.__table__
        columns = table.columns.keys()
        hosts_attribute = zcls.host
        name_attribute = zcls.name
        columns_important = getattr(config.columns, args.type)

        if args.type == ZReportGenerator.TYPE_SNAPSHOT:
            query = query.filter(ZDataset.avail == None)
            columns_important = config.columns.snapshot
        elif args.type == ZReportGenerator.TYPE_FILESYSTEM:
            query = query.filter(ZDataset.avail != None).filter(ZDataset.type == ZReportGenerator.TYPE_FILESYSTEM)
            columns_important = config.columns.filesystem

        # COLUMNS FILTER
        #################
        if args.columns:
            has_user_columns = True
            if len(args.columns) == 1 and args.columns[0] in (self.COLUMN_ALL, self.COLUMN_IMPORTANT):
                if args.columns[0] == self.COLUMN_IMPORTANT:
                    args.columns = columns_important
                else:
                    has_user_columns = False
                # end handle 'all'
            # end handle presets

            if has_user_columns:
                columns = self.verify_columns(table.columns, args.columns)
                if not columns:
                    return self.ERROR
                # end early abort
            # end handle special case: all
        # end check provided columns

        # Always use the updated_at column
        columns.insert(0, 'updated_at')

        # HOSTS FILTER
        ##############
        if args.host:
            query = query.filter(hosts_attribute == args.host)
        # end

        # Name filter
        ##############
        if args.name:
            name = '%%%s%%' % args.name
            query = query.filter(name_attribute.like(name))
        # end handle name filter

        # ORDER
        #########
        # NOTE: if there is no order, order by creation ascending !
        if not args.order_by_asc and not args.order_by_desc:
            args.order_by_asc = ['host', 'creation']
        # end auto-order

        for attr, order in (('order_by_asc', 'asc'), ('order_by_desc', 'desc')):
            order_cols = getattr(args, attr)
            if not order_cols:
                continue
            # end handle order_cols
            order_cols = self.columns_by_names(table.columns, order_cols)
            if order_cols:
                query = query.order_by(*(getattr(col, order)() for col in order_cols))
        # end for each attr, order
        
        rep = Report()
        rep.columns = self.table_schema_from_colums(table.columns, columns)
        now = datetime.now()

        # FILL RECORDS
        ##############
        col_to_attr = zcls.__mapper__.get_property_by_column
        name_to_col = table.columns.__getitem__
        for inst in query:
            rec = list()

            if isinstance(inst, ZDataset) and args.leaf and not inst.is_snapshot() and list(inst.children()):
                continue
            # end skip non-leaf datasets

            for cid, name in enumerate(columns):
                if name == self.COLUMN_URL:
                    val = str(ZFSURL.new_from_dataset(inst.host, inst.name))
                else:
                    val = getattr(inst, col_to_attr(name_to_col(name)).key)
                    if isinstance(val, datetime):
                        val = now - val
                    # end handle conversions
                # end handle special case
                rec.append(val)
            # end for each colum
            rep.records.append(rec)
        # end for each row

        # AGGREGATION
        ##################
        if len(rep.records) > 1:
            agr = rep.aggregate_record()
            agr[0] = now - now
            rep.records.append(agr)

            if args.aggregate_only:
                rep.records = rep.records[-1:]
            # end remove all records but aggregate
        # end aggregate only if there is something

        # Finally, make sure updated_at becomes seen - we now have the values and no one cares about the schema
        # names anymore
        for col in rep.columns:
            if col[0] == 'updated_at':
                col[0] = 'seen'
        # end rename updated_at

        rep.serialize(Report.SERIALIZE_TTY, sys.stdout.write)
        return self.SUCCESS

# end class ListSubCommand
