#-*-coding:utf-8-*-
"""
@package bit.tests.test_utility
@brief tests for bit.utility

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from bit.tests import ITTestCaseBase

from bit.utility import *
from time import (time,
                  sleep,
                  timezone)
from datetime import datetime


class TestUtility(ITTestCaseBase):
    __slots__ = ()

    def test_size(self):
        ki = 1024
        for size, res in (('124K', 124.0*ki), 
                          ('24.2M', 24.2*ki**2),
                          ('10.6g', 10.6*ki**3), # caps don't matter
                          ('2.75T', 2.75*ki**4),
                          ('5.234P', 5.234*ki**5) ): 
            assert size_to_int(size) == int(res)
        # end for each sample to test

    def test_conversion(self):
          """Assure we do it right !"""
          size_str = '59.6T'
          size = size_to_int(size_str)
          assert round(float(size) / 1024**4, 3) == 59.6

          now_time = time()
          offset = 83921
          delta = seconds_to_datetime(now_time) - seconds_to_datetime(now_time - offset)
          assert delta_to_seconds(delta) == offset

          # just make the call, no check for now
          utc_datetime_to_date_time_string(datetime.utcnow())

    def test_cache(self):
        """Test our simple caching class"""
        cache = ExpiringCache()

        assert len(cache) == 0

        key, val, ttl = 'foo', 1, 0.05
        assert cache.get(key) is None

        for use_update in range(2):
            assert len(cache) == 0
            assert cache.set(key, val, ttl, update_fun = use_update and (lambda k, pv: pv) or None)
            assert len(cache) == 1
            assert cache.get(key) is val
            sleep(ttl + ttl*0.1)
            assert bool(cache.get(key)) == bool(use_update)
            assert len(cache) == use_update
        # end for each use_update mode
          
    def test_user_info(self):
        """Verify user information parser works correctly"""

        class TestIDParser(ThreadsafeCachingIDParser):
            __slots__ = ('fail', 'missing_group_name', 'call_count')

            def __init__(self, fail=False, missing_group_name=False, ttl=0.0):
                super(TestIDParser, self).__init__(time_to_live=ttl)
                self.fail = fail
                self.missing_group_name = missing_group_name
                self.call_count = 0

            def _call_id(self, name):
                self.call_count += 1
                if self.fail:
                    return None
                if self.missing_group_name:
                    return """uid=10263(thielse) gid=300(domainusers) groups=300(domainusers),10(wheel),500(vboxusers),20002(INTSG-LocalAdmin),4004(role-developer),403(M3Recruiting),409(sec-2),407(),406"""
                return """uid=10263(thielse) gid=300(domainusers) groups=300(domainusers),10(wheel),500(vboxusers),20002(INTSG-LocalAdmin),4004(role-developer),403(M3Recruiting),409(sec-2),407(role-data-io),406(project-batcave)"""
        # end class TestIDParser

        parser = TestIDParser(fail=True)
        user = 'thielse'
        primarygroup = 'domainusers'
        assert parser.parse(user) is None

        parser.fail = False
        res = parser.parse(user)

        assert res.uid == (10263, user)
        assert res.gid == (300, primarygroup)
        assert len(res.groups) == 9
        grp = res.groups[0]
        assert grp[0] == 300 and grp[1] == primarygroup
        grp = res.groups[-1]
        assert grp[0] == 406 and grp[1] == 'project-batcave'

        parser.missing_group_name = True
        res = parser.parse('foo')

        for grp in res.groups[-2:]:
            assert isinstance(grp[0], int)and grp[1] is None
        # end for each group to test


        for ttl in (0.0, 1.0):
            parser.missing_group_name = False
            assert parser.set_cache_expires_after(ttl) is parser
            assert parser.parse(user).groups[-1][1] is not None
            parser.missing_group_name = True
            assert bool(parser.parse(user).groups[-1][1]) == bool(ttl), "cache should have hit, thus we should see an 'old' value"
        # end handle cache test

        class TestSpecialCase(IDParser):
            """Test out a possible bug"""
            __slots__ = ()

            def _call_id(self, name):
                return "uid=10209(schumaer) gid=300(domainusers) groups=300(domainusers),20002(INTSG-LocalAdmin),407(role-data-io)\n".strip()

        # end class TestSpecialCase

        parser = TestSpecialCase()
        res = parser.parse('schumaer')
        assert (407, 'role-data-io') in res.groups
        
# end class TestUtility
