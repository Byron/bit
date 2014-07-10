#-*-coding:utf-8-*-
"""
@package itool.cmd.report
@brief A command for generating any kind of it report

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ReportIToolSubCommand']

from .base import IToolSubCommand
from bit.cmd import ReportCommandMixin

from bit.reports import ReportGeneratorBase


class ReportIToolSubCommand(ReportCommandMixin, IToolSubCommand):
    """ZFS boilerplate for reports"""
    __slots__ = ()

    description = 'Generate reports about anything in IT'
    version = '0.1.0'

    # -------------------------
    ## @name Configuration
    # @{

    ReportBaseType = ReportGeneratorBase
    
    ## -- End Configuration -- @}

# end class ReportIToolSubCommand
