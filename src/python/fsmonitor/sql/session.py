#-*-coding:utf-8-*-
"""
@package zfs.sql.session
@brief A session implementation specifically for Z-related data

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['PackageSession', 'with_threadlocal_session']

import threading
import logging


from sqlalchemy.orm import (Session,
                            sessionmaker)
from sqlalchemy import create_engine
from .orm import (SQLPackage,
                  SQLPackageTransaction)
from bit.utility import seconds_to_datetime

from socket import gethostname
from datetime import datetime

log = logging.getLogger('zfs.sql.session')


def with_threadlocal_session(fun):
    """A decorator to bind a threadlocal session object to the last argument of a function.
    For it to work, the owning object (either instance or type) needs to have a _url attribute which points to the 
    database to connect to. We will not cache the session, but recreate it every time, relying on SQLAlchemy's connection 
    spooling to be efficient with it"""
    def wrapper(obj, *args, **kwargs):
        assert hasattr(obj, '_url'), "Caller must assure his object has the _url attribute, pointing to the database to connect to"
        args = list(args)
        session = PackageSession.new(url=obj._url, create_all=False)
        args.append(session)
        try:
            return fun(obj, *args, **kwargs)
        finally:
            session.close()
        # end assure session (and connection) is closed
    # end handle wrapper
    wrapper.__name__ = fun.__name__
    return wrapper


class PackageSession(Session):
    """A session for objects defined in our ORM. We integrate with the kvstore to obtain database information."""
    __slots__ = ()

    ## The static type we create once, seems to be wanted by sqlalchemy for optimization, so lets go with it
    _main_type = None

    # -------------------------
    ## @name Interface
    # @{

    @classmethod
    def new(cls, url = None, engine = None, create_all = True):
        """Intialize a new session - if there is no engine, we will instantiate one from the hosturl,
        We will initialize the database from our schema in any case
        @param engine the engine to use, or None
        @param hosturl sqlalchemy compatible URL at which to find the database.
        @param create_all if True, we will assure our schema exists in the database.
        """
        assert url or engine, "Need to set at least url or engine"

        if engine is None:
            engine = create_engine(url)
        # end create engine

        if cls._main_type is None:
            cls._main_type = sessionmaker(class_=cls)
        # end handle main type

        # Assure we have tables
        if create_all:
            from .orm import SQLBase
            SQLBase.metadata.create_all(engine)
        # end handle create all

        return cls._main_type(bind=engine)

    def transactions(self, condition = None):
        """@return query result with all transactions managed by this host
        @param condition an sqlalchemy compatible condition for use in a filter(). If None, filter will have no effect"""
        query = self.query(SQLPackageTransaction).filter(SQLPackageTransaction.host == gethostname())

        if condition is not None:
            query = query.filter(condition)
        # end use condition

        return query

    def to_sql_package(self, package, stable_since = None, managed_only = True):
        """@return the latest existing SQL package instance matching the given one, or create a new one if stable_since
        is provided. Else a ValueError will be thrown.
        @param package Package instance which should be persisted. May also be a tuple or list of root_path and 
        relative path, in case you don't have a package instance
        @param stable_since seconds since epoch since which the possible new instances should be considered stable.
        If None, no new instance can be created on the fly.
        @param managed_only if False, unmanaged packages will be returned as well. Otherwise unmanaged 
        packages will be ignored
        @throws ValueError if stable_since is None and no suitable instance could be found at package_root
        @note We will not commit the operation in case of the addition of an instance, as this should be part of 
        your own transactional model. However, we will add it to this session"""
        if isinstance(package, (list, tuple)):
            assert len(package) == 2, "need tuple of(tree_root, relative_path) as package substitute"
            tree_root, root_relative = package
        else:
            tree_root, root_relative = package.tree_root(), package.root_relative()
        # end handle package input type

        query = self.query(SQLPackage).filter((SQLPackage.root_path == str(tree_root)) & 
                                              (SQLPackage.package_path == str(root_relative)))
        if managed_only:
            query = query.filter(SQLPackage.unmanaged_at == None)
        # end handle allow deleted

        # In fact it's natural that we get more recent items later, but we limit the result to one and thus need
        # the latest first
        query = query.order_by(SQLPackage.managed_at.desc()).limit(1)
        res = list(query)
        if len(res) == 1:
            return res[0]
        # end handle existing

        if stable_since is None:
            raise ValueError("No existing instance found for %s, cannot create new instance without stable_since" % package)
        # end handle error

        pkg = SQLPackage(host=gethostname(), root_path=str(tree_root),
                                             package_path=str(root_relative),
                                             managed_at=datetime.utcnow(),
                                             stable_since=seconds_to_datetime(stable_since))
        self.add(pkg)
        return pkg

    def new_package_transaction(self, transaction_type, input_package):
        """@return a new package transaction suitable to represent the given transaction type
        @param input_package an sql package instance for use within the new transaction
        @param transaction_type of the transaction
        @note the created sql-instance will not be queued or started, but added to this session"""
        trans = SQLPackageTransaction(host=gethostname(), 
                                      type_name=transaction_type.plugin_name(), 
                                      in_package=input_package,
                                      in_package_stable_since=input_package.stable_since,
                                      spooled_at=datetime.utcnow())
        self.add(trans)
        return trans
        

    ## -- End Interface -- @}
# end class ZSession
