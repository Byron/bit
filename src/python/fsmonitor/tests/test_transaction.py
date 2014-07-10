#-*-coding:utf-8-*-
"""
@package fsmonitor.tests.test_transaction
@brief tests for dropbox.transaction

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DropboxTransactionBase']

from time import time
import logging
from datetime import datetime

from bkvstore import (KeyValueStoreModifier,
                      RootKey)

from . import DropboxTestCase
from .test_sql import DummyPackage

from butility.tests import with_rw_directory
from fsmonitor.sql import (PackageSession,
                         SQLPackageTransaction)
from fsmonitor.tree import TreeRoot
from fsmonitor.finder import DropboxFinder
from fsmonitor.transaction import *

log = logging.getLogger('dropbox.tests.test_transaction')


class DropboxDaemonTestCase(DropboxTestCase):
    __slots__ = ('_url')

    @with_rw_directory
    def test_base(self, rw_dir):
        """Test basic interaction"""
        self._url = 'sqlite:////%s/db.sqlite' % rw_dir
        session = PackageSession.new(url=self._url)

        tree_root_path = self.fixture_path('tree/a')
        dbf = DropboxFinder([tree_root_path])
        tree = TreeRoot(tree_root_path)
        package = tree.iter_packages().next()
        sql_package = session.to_sql_package(package, time())


        # now create a transaction and start it
        sql_trans = session.new_package_transaction(TransferDropboxTransaction, sql_package)
        session.commit()

        config = KeyValueStoreModifier(dict(mode=TransferDropboxTransaction.MODE_COPY,
                                            destination_dir=rw_dir))
        assert TransferDropboxTransaction.can_enqueue(package, sql_package, config)
        trans = TransferDropboxTransaction(log, sql_instance = sql_trans,
                                                dropbox_finder = dbf,
                                                package=package,
                                                kvstore = config)
        assert trans.sql_instance().set_queued() is sql_trans
        # TODO: be more elaborate, assert database
        assert trans.apply().succeeded()

        session.expire_all()
        assert session.query(SQLPackageTransaction).filter(SQLPackageTransaction.id == trans._sql_instance_id)[0].finished_at is not None


