#-*-coding:utf-8-*-
"""
@package zfs.cmd.report
@brief A command for generating zfs reports

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ReportZFSSubCommand']

from .base import ZFSSubCommand
from bit.cmd import ReportCommandMixin
from zfs.sql.reports import ZReportGenerator


class ReportZFSSubCommand(ReportCommandMixin, ZFSSubCommand):
    """ZFS boilerplate for reports"""
    __slots__ = ()

    description = 'Generate reports about zfs statistics'
    version = '0.1.0'

    # -------------------------
    ## @name Configuration
    # @{

    ReportBaseType = ZReportGenerator
    
    ## -- End Configuration -- @}

# end class ReportZFSSubCommand
