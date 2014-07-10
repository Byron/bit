#-*-coding:utf-8-*-
"""
@package bit.tests.test_bundler
@brief tests for bit.bundler

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from bit.tests import ITTestCaseBase
from bit.bundler import *


import sys
import os
from sqlalchemy import (create_engine,
                        MetaData,
                        select)

from marshal import (dump, load)
from datetime import datetime
from time import time

# ==============================================================================
## @name Globals
# ------------------------------------------------------------------------------
## @{

epoch = datetime(1970,1,1)
to_s = lambda d: (d.microseconds + (d.seconds + d.days * 24 * 3600) * 10**6) / 10**6

## -- End Globals -- @}


class TestBundler(ITTestCaseBase):
    __slots__ = ()

    @classmethod
    def fixture_path(cls, name):
        return super(TestBundler, cls).fixture_path(os.path.join('bundler', name + '.marshal'))

    @classmethod
    def db_url(cls):
        """@return a locally available database with filesystem statistics"""
        raise NotImplementedError("This database should either be generated on the fly or be made available")

    @classmethod
    def _table_iterator(cls):
        """@return an iterator yielding (connection, table_name, table) tuples for accessing our test database"""
        engine = create_engine(cls.db_url())
        mcon = engine.connect()
        md = MetaData(engine, reflect=True)

        for name, table in md.tables.iteritems():
            yield mcon, name, table
        # end for each item
        
    def _disabled_test_update_fixture_(self):
        """Build our fixture from a known database"""
        rcount = 100000
        
        for mcon, name, table in self._table_iterator():
            fixture_path = self.fixture_path(name)

            print >> sys.stderr, "Obtaining %i from %s" % (rcount, self.db_url() + '/' + name)
            db = list()
            for r in mcon.execute(select([table], (table.c.ctime != None) & (table.c.sha1 != None)).limit(rcount)):
                db.append((str(r[1]), r[2], to_s(r[4]-epoch), to_s(r[5]-epoch), r[10], r[13]))
            # end for each row

            dump(db, open(fixture_path, 'wb'))
        # end for each table

    def _disabled_test_big_db_performance(self):
        """Marshall entire databases into the data-structure and see how long it takes"""
        bdl = Bundler()
        for con, name, table in self._table_iterator():
            count = [0]
            st = time()
            def row_iterator():
                for r in con.execute(select([table], (table.c.ctime != None) & (table.c.sha1 != None))):
                    count[0] += 1
                    yield (str(r[1]), (r[2], to_s(r[4]-epoch), to_s(r[5]-epoch), r[10], r[13]))
                # end for each record
            # end 
            res = bdl.bundle(row_iterator())
            elapsed = time() - st

            print >> sys.stderr, "Bundled %i records from %s into %i records in %ss (%f records/s)" % (count[0], name, len(res), elapsed, count[0] / elapsed)
            fp = self.fixture_path(name + '_result')

            st = time()
            dump(res, open(fp, 'wb'))
            elapsed = time() - st
            print >> sys.stderr, "Serialized data in %ss" % elapsed

            st = time()
            del(res)
            res = load(open(fp))
            elapsed = time() - st
            print >> sys.stderr, "Deserialized data in %ss" % elapsed

            fp.remove()
        # end for each table
        
    def test_version_extraction(self):
        """test the default version regex works on typical paths"""
        for path in ('/mnt/projects/project-name/subdir/production/genf/google/01/compositing/comp_render/aa_genf_google_01_comp_v046/jpg/1280x360/right/aa_genf_google_01_comp_v046_right.0103.jpg',
                     '/mnt/projects/project-name/subdir/production/genf/ball/01/animation/work/aa_genf_ball_01_anm_wrk_herbstma_render_v002/visibleobjects/aa_genf_ball_01_diffuseenvironment_v002_right.94690.exr',
                     '/mnt/projects/project-name/subdir/mailout/20120412_simone/aa_genf_ball_01_comp_v015/dpx/4096x2048/left/aa_genf_ball_01_comp_v015_left.94716.dpx',
                     '/mnt/projects/project-name/subdir/abf/0200/01/compositing/rendertemp/dt_abf_0200_01_comp_v014_sg_120412120521.nk',
                     '/mnt/projects/project-name/subdir/1112/02/01/rendering/renderman/ec_1112_02_01_rnd_wrk_braunst_start_v011/rib/0159/fm_lens_rshape_mb.0159.75.ribc',
                     '/mnt/projects/project-name/subdir/production/1112/02/01/rendering/release/v011/fairman/ec_1112_02_01_n_v011.0196.exr',
                     '/mnt/projects/project-name/subdir/production/1112/01/01/rendering/release/v007/ec_1112_01_01_rnd_wrk_braunst_start_v007_logoidpass.0122.exr',
                     '/mnt/projects/project-name/subdir/meetingminutes/20130418_cinesync/fx/7d_330_130_01_sequenceqt_v001_stereo.mov',
                     '/mnt/projects/project-name/subdir/fx/burner_fx/3dsmax/renders/burnerfx_db_v001/burnerfx_db_v001_0014.png'
                    ):
            sp = Bundler()._extract_version_span(path)
            assert sp
        #end version
        
        
    def test_bundler(self):
        """Basic bundle tests"""
        bdl = Bundler()

        for name in ('Freezerburn_343175', '7thDwarf_243111', 'tmp'):
            db = load(open(self.fixture_path(name)))
            res = bdl.bundle(((r[0], r[1:]) for r in db))
            assert res
            res = bdl.rebuild_bundle(res)
            assert res

            # test version bundles
            for prefix, bundle in res.iteritems():
                assert bundle.version_min <= bundle.version_max
            # end for each prefix
        # end for each name

        
