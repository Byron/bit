#-*-coding:utf-8-*-
"""
@package fsmonitor.tests.base
@brief Basic types for testing

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DropboxTestCase']

from butility.tests import TestCase
from butility import Path


class DropboxTestCase(TestCase):
    """Base type for all ZFS related tests"""
    __slots__ = ()

    fixture_root = Path(__file__).dirname()

# end class DropboxTestCase
