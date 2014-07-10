#-*-coding:utf-8-*-
"""
@package zfs.tests.base
@brief Basic types for testing

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZFSTestCase']

from butility.tests import TestCase
from butility import Path


class ZFSTestCase(TestCase):
    """Base type for all ZFS related tests"""
    __slots__ = ()

    fixture_root = Path(__file__).dirname()

# end class ZFSTestCase
