#-*-coding:utf-8-*-
"""
@package dropbox.tests.test_sql
@brief tests for dropbox sql related facilities

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from . import DropboxTestCase
from tempfile import mktemp

from sqlalchemy.orm import object_session 
from sqlalchemy.orm.util import has_identity 

from tx.tests import with_rw_directory
from dropbox.sql import *
from datetime import datetime


# ==============================================================================
## @name Utilities
# ------------------------------------------------------------------------------
## @{

class DummyPackage(object):
    """Helper to allow packages to be used"""
    __slots__ = ('_tree_root', '_root_relative')

    def __init__(self, tree_root, root_relative):
        self._tree_root = tree_root
        self._root_relative = root_relative        

    def tree_root(self):
        return self._tree_root

    def root_relative(self):
        return self._root_relative        

# end class DummyPackage



## -- End Utilities -- @}


class DropboxSQLTestCase(DropboxTestCase):
    __slots__ = ()

    @classmethod
    def temporary_sqlite_url(cls, rw_dir):
        """@return to a temporary sqlite database, which is yet to be created"""
        return "sqlite:///%s" % mktemp(dir=rw_dir, suffix='.sqlite')

    @with_rw_directory
    def test_sql_session(self, rw_dir):
        """Test basic SQL handling"""
        session = PackageSession.new(url=self.temporary_sqlite_url(rw_dir))
        assert len(list(session.query(SQLPackage))) == 0
        now = datetime.now()

        # Setup a package
        pkg = SQLPackage(host='foo', root_path='path/foo', package_path='package_dir', managed_at=now, stable_since=now)

        assert len(pkg.transactions) == 0
        session.add(pkg)
        session.commit()
        assert has_identity(pkg)

        assert session.to_sql_package(pkg) is pkg

        # Create a new package, for the fun of it
        dpkg = DummyPackage('hello/world', 'package')
        self.failUnlessRaises(ValueError, session.to_sql_package, dpkg)
        other_pkg = session.to_sql_package(dpkg, stable_since=10.0)
        assert other_pkg != pkg
        assert object_session(other_pkg) is session and not has_identity(other_pkg)


        # unmanage the package and verify it's not returned
        pkg.unmanage()
        self.failUnlessRaises(ValueError, session.to_sql_package, pkg)
        assert session.to_sql_package(pkg, managed_only=False) is pkg

        # Setup a simple transaction
        trans = SQLPackageTransaction(host='foo', type_name='testing', in_package=pkg, 
                                            in_package_stable_since = pkg.stable_since, spooled_at=now)
        assert len(pkg.transactions) == 1
        assert trans.in_package is pkg
        assert trans.out_package is None
        assert len(trans.files) == 0
        session.add(trans)
        session.commit()

        # Add a transaction file
        file = SQLTransactionFile(transaction = trans, path='foo/bar', size=14, uid=2, gid=1, mode=0)
        assert len(trans.files) == 1 and trans.files[0] is file
        assert file.transaction is trans

        session.add(file)
        session.commit()

        # it will work indirectly as well
        trans.files.append(SQLTransactionFile(path='foo/bar2', size=14, uid=2, gid=1, mode=0))
        session.commit()

        ##############################
        # TODO: Unicode paths !!!! ##
        ###########################
        # This will be breaking necks otherwise
