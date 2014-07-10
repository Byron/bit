#-*-coding:utf-8-*-
"""
@package fsmonitor.cmd.daemon
@brief Implementation of a daemon command to handle dropboxes

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DaemonCommand']

from bit.cmd import DaemonCommand
from fsmonitor.daemon import DaemonThread


class DaemonCommand(DaemonCommand):
    """Main Daemon command without subcommands. Just starts a thread.
    @note could easily be a base class, lets see how many more there will be
    """
    __slots__ = ()

    # -------------------------
    ## @name Configuration
    # @{

    name = 'fsmonitor-daemon'
    description = 'A simple command to start and possibly daemonize itself for handling monitored filesystem locations'
    version = '0.1.0'

    ## The kind of TerminatableThread (being a context client) to daemonize
    ThreadType = DaemonThread

    ## -- End Configuration -- @}

# end class DaemonCommand

