#-*-coding:utf-8-*-
"""
@package tractool.db_cache
@brief a subcommand for manipulating the tractor cache

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import os
import sys

from butility import LazyMixin

from . import base
from .utility import (CacheParser,
                      CSVJob )


class InMemoryCache(CacheParser, LazyMixin):
    """A utilty type which stores the cache in-memory for easier manipulation"""
    __slots__ = (
                '_first_line',      # first line of the parsed cache file 
                'entries',          # list of tuples(jobid, username)
                '_predicate'
                )
    
    columns = ('jid', 'user')
    
    def __init__(self, root, predicate = None):
        """Intialize thise instance
        @param root database root
        @predicate predicate by which to keep entries"""
        super(InMemoryCache, self).__init__(root)
        self._first_line = None
        self._predicate = predicate
    
    def _set_cache_(self, name):
        if name == 'entries':
            self.entries = list(self.iter_entries(self._predicate))
        else:
            return super(InMemoryCache, self)._set_cache_(name)
        #end handle entries
        
    def _handle_comment(self, line):
        assert self._first_line is None, "multiple comments in cache file cannot currently be handled"
        self._first_line = line
        
    # -------------------------
    ## @name Interface
    # @{
    
    def stream_as_native(self, stream):
        """Represent our data in native format
        @note for now, always uses linux line separators"""
        # heat cache
        self.entries
        assert self._first_line, 'first comment line should have been parsed previously'
        stream.write(self._first_line + '\n')
        for entry in self.entries:
            stream.write('%i,  %s\n' % entry)
        # end for each entry
    
    def stream_as_csv(self, stream):
        """Print all our entries to the given stream, without header"""
        for entry in self.entries:
            stream.write(CSVJob.field_sep.join(str(field) for field in entry) + os.linesep)
        # end for each entry
    ## -- End Interface -- @}
    
    
# end class CacheStorage


class DBCacheSubCommand(base.TractorDBCommand, Plugin):
    """Implemnts listing the raw database in various ways"""
    __slots__ = ()
    
    # -------------------------
    ## @name Baseclass Configuration
    # @{
    
    name = 'db-cache'
    description = 'prune the contents of the tractor cache based on some criteria. By default, we print the cache as CSV by date'
    version = '0.1.0'
    
    ## -- End Baseclass Configuration -- @}
    
    # -------------------------
    ## @name Configuration
    # @{
    
    output_mode_csv = 'csv'
    output_mode_native = 'native'
    output_modes = (output_mode_csv, output_mode_native)
    
    ## -- End Configuration -- @}
    
    def setup_argparser(self, parser):
        super(DBCacheSubCommand, self).setup_argparser(parser)
        help = "Specifiy whether to output the data as csv or in native cache format. Native format is suitable"
        help += "for actual cache replacement."
        parser.add_argument('-o', '--output-mode', dest='output_mode', choices=self.output_modes, 
                            default=self.output_mode_native, help=help)
        
        help = "All jobs older than the given Job ID (YYMMDDUUUU) will be dropped from the output"
        parser.add_argument('--drop-older-than', type=int, default=None, dest='minimum_age', help=help)
        return self
        
    def execute(self, args, remaining_args):
        # Verification
        ###############
        if args.skip_header and args.output_mode != self.output_mode_csv:
            self.log().error("skip header has no effect unless csv output is chosen")
            return self.ERROR
        #end handle skip header logic
        
        predicate = None
        if args.minimum_age:
            predicate = lambda jid, user: jid >= args.minimum_age
        #end handle minimum age predicate
        cache = InMemoryCache(args.root, predicate)
        
        if args.output_mode == self.output_mode_csv:
            if not args.skip_header:
                sys.stdout.write(CSVJob.field_sep.join(InMemoryCache.columns) + os.linesep)
            # end handle headers
            cache.stream_as_csv(sys.stdout)
        else:
            cache.stream_as_native(sys.stdout)
        # end handle output mode
        return self.SUCCESS
        

# end class DBCacheSubCommand
