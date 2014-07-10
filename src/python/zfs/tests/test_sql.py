#-*-coding:utf-8-*-
"""
@package zfs.tests.test_parse
@brief tests for zfs.parse

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from time import time

from tx.tests import with_rw_directory
from zfs.tests import ZFSTestCase
from zfs.sql import *
from zfs.parse import (
                            AdaptiveColumnParser,
                            ZPoolOmniOSParser,
                            ZPoolSolarisParser
                      )

from sqlalchemy import (
                            create_engine
                        )

class SQLSynTestCase(ZFSTestCase):
    __slots__ = ()

    @with_rw_directory
    def test_sync(self, tmpdir):
        """Convert different states and from zfs output and sync a database with it"""
        engine = create_engine('sqlite:///%s/zdb.sqlite' % tmpdir)
        session = ZSession.new(engine=engine)

        def check_inst_count(SqlType, host, expected_count):
            assert session.query(SqlType).filter(SqlType.host == host).count() == expected_count
        # end utility
        
        for parser, fixture, SqlType in ((AdaptiveColumnParser, 'dataset_list.fs5', ZDataset),
                                         (AdaptiveColumnParser, 'dataset_list.bs3', ZDataset),
                                         (ZPoolOmniOSParser, 'zpool_list.bs3', ZPool),
                                         (ZPoolSolarisParser, 'zpool_list.bs2', ZPool)):
            reader = open(self.fixture_path('cmd/%s' % fixture))
            pi = parser()

            host = fixture.split('.')[-1]
            samples = list(pi.parse_stream(reader))

            st = time()
            # Add
            session.sync(host, samples, SqlType).commit()
            check_inst_count(SqlType, host, len(samples))

            # Delete a lot
            session.sync(host, samples[:1], SqlType).commit()
            check_inst_count(SqlType, host, 1)

            # Update
            changed_version = 999
            for eid, entry in enumerate(samples[0]):
                if entry[0] == 'version':
                    samples[0][eid] = (entry[0], changed_version)
                    break
                # end change value
            # end for each column
            session.sync(host, samples[:1], SqlType).commit()

            assert session.query(SqlType).filter(SqlType.host == host).filter(SqlType.name == samples[0][0][1]).first().version == changed_version

            elapsed = time() - st
            print "Added, deleted and updated %i records in %fs (%f records/s)" % (len(samples), elapsed, len(samples) / elapsed)
        # end for each dataset

        # verify custom attributes are actually being submitted
        parser = AdaptiveColumnParser()
        def assert_recv_count_url(condition):
            assert (len(list(session.query(ZDataset).filter(ZDataset.tx_receive_url != None))) == 0) == condition

        assert_recv_count_url(True)
        session.sync('fsx', parser.parse_stream(open(self.fixture_path('cmd/dataset_list.fs4'))), ZDataset)
        assert_recv_count_url(False)


