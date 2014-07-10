#-*-coding:utf-8-*-
"""
@package bit.tests.base
@brief Base classes and utilities for use by test-cases

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ITTestCaseBase']

from butility.tests import TestCase
from butility import Path


class ITTestCaseBase(TestCase):
    """Base type for all IT related test"""
    __slots__ = ()

    fixture_root = Path(__file__).dirname()

# end class ITTestCaseBase

