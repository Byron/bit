#-*-coding:utf-8-*-
"""
@package zfs.tests.test_sql_reports
@brief tests for zfs.sql.reports

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from zfs.tests import ZFSTestCase
from zfs.sql.session import ZSession
from zfs.sql.orm import (ZPool,
                         ZDataset)
from zfs.sql.reports import *
from zfs.url import ZFSURL
from sqlalchemy import create_engine
from bit.utility import seconds_to_datetime
from time import time

from butility.compat import StringIO


class ReportsTestCase(ZFSTestCase):
    __slots__ = ()

    ## a new session instance pointing to our (read-only) database
    session = ZSession.new(engine=create_engine('sqlite:///%s' % ZFSTestCase.fixture_path('sql/zdb.sqlite')))

    def _assert_rep_not_empty(self, ReportType, config):
        """Create a new instance of type report type, and verify it's report capabilities"""
        gen = ReportType(self.session, config)
        report = gen.generate()
        assert not report.is_empty()

        sio = StringIO()
        if gen.generate_fix_script(report, sio.write):
            assert sio.getvalue()
        # end verify fix script

    def test_limits(self):
        """Verify the limit check works as expected"""
        config = ZLimitsReportGenerator.settings_value()
        config.max_pool_percent = config.max_filesystem_percent = 80.0
        config.min_filesystem_avail_size = '5G'
        config.min_snapshot_size = config.min_filesystem_size = '100k'
        config.snapshots_older_than_days = config.filesystems_older_than_days = 180

        self._assert_rep_not_empty(ZLimitsReportGenerator, config)

    def test_retention_policy(self):
        """Test the policy works correctly"""
        for pstring in ('10', '10s:', '10k', '1h:1d,30s:14d'):
            self.failUnlessRaises(ValueError, RetentionPolicy, pstring)
        # end for each string to fail
        policy = RetentionPolicy('10s:14d')
        policy = RetentionPolicy('1h:1d,1d:14d,14d:28d,30d:1y')

        # verify filter computation
        sc, isc = policy.num_rule_samples()
        tsc = 24 + 14 + 2 + 365/30
        assert sc == tsc

        sc, isc = policy.num_rule_samples(period='2d')
        assert sc == tsc
        assert isc == 24 + 1

        now = time()
        def sample(age):
            return seconds_to_datetime(now - age), None
        # end utility

        samples = [sample(0), sample(3600),                         # 1h:1d, 2 samples
                   sample(3600*24), sample(3600*24*2),              # 1d:14d, 2 samples
                   sample(3600*24*15),                              # 14d:28d, 1 sample
                   sample(3600*24*(15+28)),                         # 30d:1y
                  ]

        # Nothing should have been removed
        assert policy.filter(now, list(reversed(samples)), ordered=True) == policy.filter(now, samples, ordered=False)
        ns, ds = policy.filter(now, samples)
        assert ns == samples
        assert not ds

        # Samples in the future stay
        samples.insert(0, sample(-5))
        ns, ds = policy.filter(now, samples)
        assert ns == samples
        assert not ds
        
        # And one more outside the retention period, which is kept though
        samples.append(sample(3600*24*720))
        ns, ds = policy.filter(now, samples)
        assert ns == samples
        assert not ds

        # If the last retention period is full, it will not be kept though
        out_of_range_sample = samples[-1]
        samples = samples[:-1]

        # We already have one sample in the period, just add 11 more
        keep, freq, duration = policy.rules()[-1]
        assert keep == 0
        samples.extend(sample(3600*24* (15+28+(i+1)*10)) for i in range((duration/freq) - 1))
        samples.append(out_of_range_sample) # re-add the out of range sample

        ns, ds = policy.filter(now, samples)
        assert ns == samples[:-1]
        assert len(ds) == 1 and ds[0] == out_of_range_sample

        # fill up 14d:28d with the second sample, place it perfectly
        samples.insert(6, sample(3600*24*(15+28/2)))
        ns, ds = policy.filter(now, samples)
        assert ns == samples[:-1]

        # Now add one extra sample into the filled region between both of the perfectly placed ones
        samples.insert(6, sample(3600*24*(15+(28/4))))
        ns, ds = policy.filter(now, samples)
        assert len(ds) == 2 and ds[0] == samples[6]

        del(samples[6])

        # And add another one, this time past the last perfectly placed one.
        # There are no special rules to keep this one, it really just wants to have a good balance
        # Thus its being removed subsequently
        samples.insert(7, sample(3600*24*(15+((28/4)*3))))
        ns, ds = policy.filter(now, samples)
        assert len(ds) == 2 and ds[0] == samples[7]

        # Now we want to enforce to keep the sample we would drop otherwise
        policy = RetentionPolicy('1h:1d,1d:14d,1:14d:28d,30d:1y')
        ns, ds = policy.filter(now, samples)
        assert len(ds) == 1 and len(ns) == len(samples) - 1

        # verify we can keep only one sample if we specify no policy
        policy = RetentionPolicy('1-')
        ns, ds = policy.filter(now, samples)
        assert len(ns) == 1 and len(ds) == len(samples) - 1

        assert policy.num_rule_samples()[0] == 1

    def test_retention(self):
        """Verify retention policy report"""
        config = ZRetentionReportGenerator.settings_value()
        config.policy = '1h:1d,1d:14d,14d:28d,30d:1y'
        config.name_like = '%GlobalScripts%'
        self._assert_rep_not_empty(ZRetentionReportGenerator, config)

    def test_url(self):
        """Verify our URL type can handle all the various names"""
        pid = 0

        # TEST POOLS
        ############
        for pid, pool in enumerate(self.session.query(ZPool)):
            url = ZFSURL.new(pool.host, pool.name)
            assert url.pool() == pool.name
            assert url.parent_filesystem_url() is None
            assert url.host() == pool.host
            assert url.filesystem() == pool.name, 'each pool has an associated filesystem'
            assert url.is_pool()
            assert not url.is_snapshot()
            assert url.snapshot() is None
            assert url.name() == pool.name
            assert ZFSURL(str(url)) == url
            assert pool.url() == url
            assert self.session.instance_by_url(url) is pool

            # Just try some additional functionality
            assert not url.joined('foo').is_pool()
        # end for each pool
        assert pid

        # TEST TRAVERSAL
        ################
        pool = self.session.instance_by_url(ZFSURL('zfs://fs5/internal'))
        assert isinstance(pool, ZPool)
        pfs = pool.as_filesystem()
        assert pfs.is_pool_filesystem()
        assert pfs.parent() is None
        assert isinstance(pfs, ZDataset)
        assert pfs.as_pool() is pool
        assert pfs.pool() is pool

        for child in pfs.children():
            assert child.parent() is pfs
            assert not child.is_pool_filesystem()
            assert child.pool() is pool
            ss = None
            for ss in child.snapshots():
                assert ss.is_snapshot()
                assert ss.parent() is child
            # end check snapshot
            if ss:
                assert child.latest_snapshot() is ss
            # end check latest snapshot
        # end verify parent/child relationships


        # TEST DATASETS
        ################
        sid = 0
        for sid, ds in enumerate(self.session.query(ZDataset)):
            tokens = ds.name.split('/', 1)
            if len(tokens) == 1:
                pool = fs = tokens[0]
            else:
                pool, fs = tokens
            # end handle pool == filesystem
            tokens = ds.name.split('@')
            fs = tokens[0]
            ss = None
            assert len(tokens) < 3
            if len(tokens) == 2:
                ss = tokens[1]
            # end handle token
            url = ds.url()
            assert url.pool() == pool
            assert url.host() == ds.host
            assert url.name() == ds.name
            assert url.filesystem() == fs
            assert url.snapshot_name() == ss
            assert not url.is_pool()
            assert url.is_snapshot() == (ss is not None)
            assert (url.parent_filesystem_url() is None) == (ds.name.count('/') == 0)
            assert url.joined('foo/bar').is_snapshot() == (ss is not None)
            if ss is None:
                assert url.snapshot() is None
            else:
                assert url.snapshot() == ds.name, 'should be fully qualified name'
            assert ZFSURL(str(url)) == url
            assert ZFSURL.new_from_dataset(ds.host, ds.name) == url
        # end for each dataset
        assert sid
        
    def test_url(self):
        """Verify url functionality"""
        # NOTE: most of the features are tested with real objects above
        url = ZFSURL.new('hostname', 'store', 'foo_bar_fs?send_args=-R&recv_args=-F')
        fields = url.query_fields()
        assert len(fields) == 2
        assert fields['send_args'] == '-R'

    def test_duplication(self):
        """check the duplication report"""
        config = ZDuplicationReportGenerator.settings_value()
        self._assert_rep_not_empty(ZDuplicationReportGenerator, config)

    def test_reserve(self):
        """check the reserve report"""
        config = ZReserveReportGenerator.settings_value()
        self._assert_rep_not_empty(ZReserveReportGenerator, config)
        
