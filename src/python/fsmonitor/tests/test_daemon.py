#-*-coding:utf-8-*-
"""
@package fsmonitor.tests.test_daemon
@brief tests for dropbox.daemon

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from . import DropboxTestCase

from nose import SkipTest
from fsmonitor.daemon import *


class DropboxDaemonTestCase(DropboxTestCase):
    __slots__ = ()


    def test_base(self):
        """Test basic daemon handling"""
        # Currently this is only tested manually, unfortunately ... 
        raise SkipTest()
        
