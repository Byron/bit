#-*-coding:utf-8-*-
"""
@package bit.tests.test_reports
@brief tests for bit.reports

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from bit.tests import ITTestCaseBase

from bit.reports import *


class ReportTests(ITTestCaseBase):
    """Very generic Report testing"""
    __slots__ = ()

    def test_base(self):
        # For now, we just test the import itself, serialization is indirectly tested by the zfs tests
        pass

# end class ReportTests

