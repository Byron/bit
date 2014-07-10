#-*-coding:utf-8-*-
"""
@package itool.base
@brief Basic functionality

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['IToolCommand', 'IToolSubCommand']

import sys

# Force encoding to something common
reload(sys)
sys.setdefaultencoding('utf-8')

import tx

from tx.cmd import (
                        CommandBase,
                        SubCommandBase
                   )



class IToolCommand(CommandBase):
    """Main command to pick up some subcommands and possibly provide some shared arguments"""
    __slots__ = ()

    name = 'itool'
    description = 'A utility to query and edit statistical caches of many types'
    version = '0.1.0'
    
    subcommands_title = 'Modes'
    subcommands_help = 'Get more help using <mode> --help'

# end class Name


class IToolSubCommand(SubCommandBase):
    """A basic subcommand for the itool"""
    __slots__ = ()
    
    main_command_name = IToolCommand.name

# end class IToolSubCommand



