#-*-coding:utf-8-*-
"""
@package bit.tests.base
@brief Base classes and utilities for use by test-cases

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ITTestCaseBase']

from tx.tests import TestCaseBase
from butility import make_path


class ITTestCaseBase(TestCaseBase):
    """Base type for all IT related test"""
    __slots__ = ()

    fixture_root = make_path(__file__).dirname()

# end class ITTestCaseBase

