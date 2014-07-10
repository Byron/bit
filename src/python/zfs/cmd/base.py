#-*-coding:utf-8-*-
"""
@package zfs.cmd.base
@brief main command implementation

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZFSCommand', 'ZFSSubCommand', 'OverridableZFSSubCommand']

import tx
from tx.cmd import (
                        CommandBase,
                        SubCommandBase
                    )

from bit.cmd import OverridableSubCommandMixin

class ZFSCommand(CommandBase):
    """Main ZFS command, does nothing as it has sub-commands"""
    __slots__ = ()

    name = 'ztool'
    description = 'A command to deal with everything related to zfs within our infrastructure'
    version = '0.1.0'

    subcommands_title = 'Modes'
    subcommands_help = 'Get context specific help using: <mode> --help'

# end class ZFSCommand


class ZFSSubCommand(SubCommandBase):
    """A subcommand to the ZFS Main command"""
    __slots__ = ()

    main_command_name = ZFSCommand.name

# end class ZFSSubCommand


class OverridableZFSSubCommand(OverridableSubCommandMixin, ZFSSubCommand):
    """A command which uses a KVStore to read its arguments from.

    This command allows overriding those arguments using the commandline.
    """
    __slots__ = ()

# end class OverridableZFSSubCommand
