#-*-coding:utf-8-*-
"""
@package zfs.tests.test_parse
@brief tests for zfs.parse

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from time import time

from zfs.tests import ZFSTestCase
from zfs.parse import *

class ParseZFSTestCase(ZFSTestCase):
    __slots__ = ()

    def test_column_parsers(self):
        """Bulk test for all column parsers"""

        for parser, fixture in ((AdaptiveColumnParser, 'dataset_list.osx'),
                                (AdaptiveColumnParser, 'dataset_list.ss1'),
                                (AdaptiveColumnParser, 'dataset_list.fs5'),
                                (AdaptiveColumnParser, 'dataset_list.bs3'),
                                (ZPoolOmniOSParser, 'zpool_list.bs3'),
                                (ZPoolSolarisParser, 'zpool_list.bs2'),
                                (ZPoolOmniOSLatestVersionParser, 'zpool_list.ss1'),
                                (ZFSListParser,         'zfs_list'),
                                (ZFSListSnapshotParser, 'zfs_list_snapshot')):
            reader = open(self.fixture_path('cmd/%s' % fixture))
            pi = parser()
            count = 0
            st = time()
            for count, converted in enumerate(pi.parse_stream(reader)):
                # we are loose here ... but its okay
                assert converted and len(converted) == len(pi.schema)
                assert len(set(k for k,v in converted)) == len(pi.schema)
            # verify consistency
            assert count
            elapsed = time() - st
            print "%s: Parsed %i samples in %f s (%f samples/s)" % (parser.__name__, count, elapsed, count / elapsed)
        # end for each parser
# end class ParseZFSTest
