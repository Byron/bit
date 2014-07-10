#-*-coding:utf-8-*-
"""
@package tractool.base
@brief Basic functionality

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['TractoolCommand', 'TractoolSubCommand', 'TractorDBCommand']

import sys

import TrContext

from bcmd import (Command,
                  SubCommand)

from butility import Path


class TractoolCommand(Command):
    """Main command to pick up some subcommands and possibly provide some shared arguments"""
    __slots__ = ()

    name = 'tractool'
    description = 'A utility to query and edit tractor'
    version = '0.1.0'
    
    subcommands_title = 'Modes'
    subcommands_help = 'Get more help using <mode> --help'

# end class Name


class TractoolSubCommand(SubCommand):
    """A basic subcommand for the tractool"""
    __slots__ = ()
    
    main_command_name = TractoolCommand.name

# end class TractoolSubCommand



class TractorDBCommand(TractoolSubCommand):
    """A base class for sub commands that interact with the tractor db directly. 
    
    It provides common arguments""" 
    __slots__ = ()
    
    tractor_db_root_default = '/var/spool/tractor'
    
    def setup_argparser(self, parser):
        parser.add_argument('-r', '--root', 
                            help='Root of the tractor database, defaults to %s' % self.tractor_db_root_default, 
                            default=self.tractor_db_root_default, type=Path,  dest='root')
        
        help = "If set, the csv header will not be printed"
        parser.add_argument('--skip-header', action='store_true', default=False, dest='skip_header', help=help)
        return self
    


# end class TractorDBCommand
