#-*-coding:utf-8-*-
"""
@package zfs.cmd.convert
@brief Converter command implementation

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ConverterZFSSubCommand']

import sys
from time import time

from urlparse import urlsplit

from .base import ZFSSubCommand
from zfs.parse import   (ZPoolOmniOSParser,
                         ZPoolSolarisParser,
                         ZPoolOmniOSLatestVersionParser,
                         AdaptiveColumnParser)
from bkvstore import KeyValueStoreSchema
from zfs.sql import (ZSession,
                     ZPool,
                     ZDataset)
from bit.utility import (graphite_submit,
                         CARBON_PORT)


# -------------------------
## @name Utilities
# @{

csv_sep = ';'

def csv_convert(host, sample):
    """Convert a sample  into a csv formatted line with trailing newline and print it"""
    sys.stdout.write(host + csv_sep)
    sys.stdout.write(csv_sep.join(str(v) for n,v in sample) + '\n')

## -- End Utilities -- @}


class GraphiteConverter(ApplicationSettingsMixin):
    """A converter taking samples and converting them into a structure suitable for consumption by graphite.

    The information provided by the parser should just contain volatile information associated with filesystems
    or pools.
    """
    __slots__ = ()

    _schema = KeyValueStoreSchema('graphite', {'carbon' :  
                                                    { 'host' : 'unknown_host',
                                                      'port' : CARBON_PORT }})

    zpool_metrics       = ('size', 'free', 'alloc', 'cap', 'health', 'dedup')
    zfilesystem_metrics = ('used', 'avail', 'refer', 'ratio', 'quota', 'reserv')

    def send(self, timestamp, host, samples, ztype):
        """Convert the samples into a carbon sample tree and send it to the carbon server. 
        The tree is looking like this:

        - hosts.<hostname>.zfs.pools.<pool-name>.metric
        - hosts.<hostname>.zfs.filesystems<filesystem-path>.metric

        @param samples an iterator yielding samples of the respective datatype
        @param host name of the host whose samples we are looking at
        @param timestamp time since epoch at which the samples were taken
        @return this instances
        """
        gsamples = list()
        graphite = self.settings_value()

        metrics, subdir = ztype is ZPool and (self.zpool_metrics, 'pools') or (self.zfilesystem_metrics, 'filesystems')
        fmt = 'hosts.%%s.zfs.%s.%%s.' % subdir
        for sample in samples:
            sample = dict(sample)
            key = fmt % (host, sample['name'].replace('/', '.'))
            if '@' in key:
                continue
            # end ignore snapshots (even if they are part of the input)
            for metric in metrics:
                val = sample[metric]
                if val is None:
                    continue
                gsamples.append((key + metric, (timestamp, val)))
            # end for each metric
        # end for each sample
        graphite_submit(graphite.carbon.host, gsamples, port=graphite.carbon.port)
# end class GraphiteConverter



class ConverterZFSSubCommand(ZFSSubCommand, Plugin):
    """Allows to convert the output of standard zfs commandline tools into particular outputs, like CSV or SQL"""
    __slots__ = ()

    name = 'convert'
    description = 'Convert zfs commandline output to CSV or SQL'
    version = '0.1.0'


    # -------------------------
    ## @name Constants
    # @{

    input_command_map = { 'all_dataset_properties' :  (AdaptiveColumnParser, ZDataset),
                          'zpool_list_solaris' : (ZPoolSolarisParser, ZPool),
                          'zpool_list_omnios' : (ZPoolOmniOSParser, ZPool) ,
                          'zpool_list_omnios_latest' : (ZPoolOmniOSLatestVersionParser, ZPool) }

    FORMAT_CSV = 'csv'
    FORMAT_SQL = 'sql-sync'
    FORMAT_GRAPHITE = 'graphite'
    FORMAT_SQL_GRAPHITE = FORMAT_SQL + '+' + FORMAT_GRAPHITE
    output_formats = (FORMAT_SQL, FORMAT_CSV, FORMAT_GRAPHITE, FORMAT_SQL_GRAPHITE)

    ## -- End Constants -- @}

    def setup_argparser(self, parser):
        super(ConverterZFSSubCommand, self).setup_argparser(parser)

        help = 'Defines the source of the data as fully qualified domain name, like hostname.domain.intern'
        parser.add_argument('-sh', '--source-host', type=str, required=True, dest='host', help=help)

        help = 'Specify the type of command line output to be read from stdin.'
        help += "The idea is to pipe the output of a command like 'zfs list' to the converter."
        help += "There are two kinds of "
        help += 'all_dataset_properties: zfs list -r -t all -o all or zpool list -o all;'
        help += 'zpool_list_omnios|zpool_list_solaris: zpool list -o all -H;'
        parser.add_argument('-f', '--from', choices=self.input_command_map.keys(), help=help, dest='from_cmd', required=True)

        help = 'Specify to which format you want to convert the input to. Valid formats are:'
        help += 'csv: comma separated values to stdout'
        parser.add_argument('-t', '--to', choices=self.output_formats, help=help, dest='format', required=True)

        return self 

    def execute(self, args, remaining_args):
        if sys.stdin.isatty():
            raise AssertionError("This command reads all values from standard input - please redirect the output of a zfs command")
        # end must not have tty

        ParserType, ZType = self.input_command_map[args.from_cmd]
        parser = ParserType()

        if args.format == self.FORMAT_CSV:
            sys.stdout.write('host' + csv_sep)
            if parser.schema is None:
                parser.parse_schema(sys.stdin.readline())
            # end assure schema is dynamically parsed as needed
            sys.stdout.write(csv_sep.join(n for n,c in parser.schema) + '\n')
            # end header

            for sample in parser.parse_stream(sys.stdin):
                csv_convert(args.host, sample)
            # end for each sample
        else:
            samples = list(parser.parse_stream(sys.stdin))
            if args.format in (self.FORMAT_SQL, self.FORMAT_SQL_GRAPHITE):
                session = ZSession.new()
                session.sync(args.host, samples, ZType).commit()
            # end handle sql/graphite
            if args.format in (self.FORMAT_GRAPHITE, self.FORMAT_SQL_GRAPHITE):
                conv = GraphiteConverter()
                conv.send(time(), args.host, samples, ZType)
            # end handle sql/graphite
        # handle any other format than csv

        return self.SUCCESS

# end class ConverterZFSSubCommand
