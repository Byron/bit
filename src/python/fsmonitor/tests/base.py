#-*-coding:utf-8-*-
"""
@package dropbox.tests.base
@brief Basic types for testing

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DropboxTestCase']

from tx.tests import TestCaseBase
from butility import make_path


class DropboxTestCase(TestCaseBase):
    """Base type for all ZFS related tests"""
    __slots__ = ()

    fixture_root = make_path(__file__).dirname()

# end class DropboxTestCase
