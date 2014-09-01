#-*-coding:utf-8-*-
"""
@package fsmonitor.daemon.utility 
@brief Various helpers used by the dropbox daemon

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
# NOTE: nothing here is officially exported, it's somewhat private to the daemon
__all__ = []

from datetime import datetime
from time import time
import logging
import threading

from bkvstore import ChangeTrackingKeyValueStoreModifier
from bit.utility import (TerminatableThread,
                           WorkerThread,
                           seconds_to_datetime,
                           datetime_to_seconds)

from fsmonitor.finder import DropboxFinder
from fsmonitor.tree import PackageDiffer
from fsmonitor.sql import (PackageSession,
                         SQLPackageTransaction,
                         with_threadlocal_session)

from fsmonitor.transaction import DropboxTransactionBase

log = logging.getLogger('dropbox.daemon')


# -------------------------
## @name Utilities
# @{

def log_package_handler_exception(fun):
    """Logs all exceptions and ignores them. Expects first argument to be a Package"""
    def wrapper(self, package, *args, **kwargs):
        try:
            return fun(self, package, *args, **kwargs)
        except Exception:
            log.error("Handling of %s failed", package, exc_info=True)
        # end handle exception
    # end wrapper        
    wrapper.__name__ = fun.__name__
    return wrapper


class ResultLoggerThread(TerminatableThread):
    """A thread which pulls from worker's result queue, logging the result accordingly

    Currently it will only log exceptions.
    It's just a consumer to assure the output queue doesn't run full (or consume vast amount of memory)
    @note we are a daemon thread and will not block a shutdown of python
    """
    __slots__ = ('inq',)

    def __init__(self, inq):
        """Initialize this instance
        @param inq Queue from which to pull WorkerThread's results"""
        super(ResultLoggerThread, self).__init__()
        self.daemon = True
        self.inq = inq

    def run(self):
        while True:
            if self._should_terminate():
                break
            # end handle abort requests

            # Note: Don't block forever, as we have to keep responding to termination requests
            res = self.inq.get()
            if isinstance(res, Exception):
                log.error("A task failed with error: %s", str(res))
            # end handle exceptions
        # end run forever
# end class ResultLoggerThread


class SessionWorkerThread(WorkerThread):
    """A worker thread which will create a thread-local session object that our implementations will use 
    to interact with the database.
    Also we will only listen to shutdown requests coming through the worker queue.
    """
    __slots__ = ('_url')

    def __init__(self, *args, **kwargs):
        """Initialize our url by providing it as a similarly named keyword argument"""
        self._url = kwargs.pop('url')
        super(SessionWorkerThread, self).__init__(*args, **kwargs)

    def cancel(self):
        """Only cancel through the queue, to assure we handle what has to be handled.
        Otherwise un-finished jobs will remain queued and confuse the logic if the daemon goes down in the meanwhile, 
        and restarts."""
        self.inq.put(self.quit)
    
# end class SessionWorkerThread


class DaemonDropboxFinderMixin(DropboxFinder):
    """An implementation which deals with deletions of dropboxes properly.
    It is expected to be a mixin to the DropboxDaemon, which needs the SQLPackageDifferMixin"""
    __slots__ = ()

    # -------------------------
    ## @name Subclass Overrides
    # @{

    def _dropbox_removed(self, stat, dropbox):
        """Assures all packages are marked deleted"""
        for tree in dropbox.trees():
            for package in tree.iter_packages():
                self._handle_removed_package(package)
            # end for each package
        # end for each tree
    
    ## -- End Subclass Overrides -- @}

# end class DaemonDropboxFinderMixin


class SQLPackageDifferMixin(PackageDiffer):
    """A simple type to react to package change callbacks when comparing different package samples.

    It is expecting to be a mixin to the DropboxDaemon
    """
    __slots__ = ()      # queue to place operations on

    thread_local = threading.local()


    # -------------------------
    ## @name Overrides
    # @{

    @with_threadlocal_session
    def diff(self, lhs, rhs, session):
        """Similar to base implementation, but our thread-local instance before making the call"""
        self.thread_local.session = session
        return super(SQLPackageDifferMixin, self).diff(lhs, rhs)

    ## -- End Overrides -- @}

    # -------------------------
    ## @name Utilities
    # @{

    def _unfinished_transactions_for(self, session, package_id):
        """@return a list of sql transaction objects for given package_id.
        @note all of which are linked to package_id, are not finished, and are not queued."""
        return list(session.transactions((SQLPackageTransaction.in_package_id == package_id) & 
                                         (SQLPackageTransaction.finished_at == None) & 
                                         (SQLPackageTransaction.percent_done == None)))

    def _transaction_cls_by_name(self, classes, name):
        """@return a cls going by the given name, or None if no such transaction existed"""
        for cls in classes:
            if cls.plugin_name() == name:
                return cls
        # end for each cls
        log.error("No plugin found to deal with transaction of name '%s'", name)

    def merged_kvstore(self, trans_cls, db):
        """@return a merged kvstore for the given transaction type. Will handle all cases correctly
        @param trans_cls a DropboxTransactionBase compatible class whose schema to use
        @param db Dropbox from which to pull configuration"""
        our_key = '%s.%s.%s' % (self.settings_schema().key(), db.TRANSACTIONS_KEY, trans_cls.plugin_name())
        db_key = '%s.%s' % (db.TRANSACTIONS_KEY, trans_cls.plugin_name())

        # We don't care if we have it our not, and allow initialization.
        # However, we will not resolve anything, as this can be done when the transaction queries its data
        kvstore = ChangeTrackingKeyValueStoreModifier(bapp.main().context().settings().value(our_key, trans_cls.schema))
        kvstore.set_changes(db.settings_kvstore().value(db_key, trans_cls.schema))
        return kvstore
    # end utility

    def _handle_possibly_stable_package(self, package, session, sql_package = None):
        """Apply our logic to see if the package can be deemed stable, and if so, do something with it 
        based on our configuration
        @param package a standard dropbox package instance
        @param session our sql session
        @param sql_package if not None, the sql_package belonging to our package. It's merely a way not to 
        reuse previous work"""
        db = self.dropbox_by_contained_path(package.root())

        # ignore dropboxes which don't have operations configured
        if not db.settings_kvstore().has_value(db.TRANSACTIONS_KEY):
            log.debug("%s didn't configure any transaction - package %s will not be handled", db, package)
            return
        # end skip dropboxes without configuration


        if time() - package.stable_since() < db.settings().package.stable_after.seconds:
            return
        # if package is not stable

        # Assume the package already exists in the database
        try:
            sql_package = sql_package or session.to_sql_package(package)
        except ValueError:
            sql_package = session.to_sql_package(package, package.stable_since())
            log.warn("%s didn't have an associated SQL package - this shouldn't happen at this point if DB is consistent - created new sql-package", package)
        # end handle inconsistency

        # Get all unfinished transactions unqueued that use this package as input
        trs = self._unfinished_transactions_for(session, sql_package.id)

        # CHECK EXISTING TRANSACTIONS
        ##############################
        # NOTE: This is done in a separate scheduler to be decoupled from package update scheduling times

        # Schedule new transactions
        # There are no transactions to check, so we have to see if we can create one based on the dropbox
        # configuration
        if not trs:
            clss = bapp.main().context().types(DropboxTransactionBase)
            # Workaround our 'AnyKey' issue
            db_config = db.settings_kvstore().data()
            for trans_name in db_config.get(db.TRANSACTIONS_KEY, dict()):
                trans_cls = self._transaction_cls_by_name(clss, trans_name)
                if not trans_cls:
                    log.warn("Package %s specified unknown transaction: '%s'", package, trans_name)
                    continue
                # end skip missing transaction types

                config = self.merged_kvstore(trans_cls, db)
                if trans_cls.can_enqueue(package, sql_package, config):
                    sql_trans = session.new_package_transaction(trans_cls, sql_package)
                    # commit here to be sure we have a valid id - the latter needs to be stored to make it
                    # into our other thread which performs the operation
                    sql_trans.commit()
                    
                    log.info("Setting up new transaction: %s", sql_trans)

                    if trans_name in db.settings().auto_approve:
                        trans = trans_cls(log, sql_instance = sql_trans,
                                               dropbox_finder = self,
                                               package = package,
                                               kvstore = config)
                        log.info("Queueing auto-approved transaction %s", trans)
                        sql_trans.set_queued().commit()
                        self._ops_queue.put(trans.apply)
                    else:
                        sql_trans.request_approval().commit()
                    # end handle auto-approve

                    # only approve the first possible transaction, let's only have one at a time
                    break
                else:
                    log.debug("Skipping '%s' transaction as it can't be enqueued yet", trans_name)
                # end transaction can be enqueued
            # end for each transaction type name
        # end handle new transaction
        
    
    ## -- End Utilities -- @}

    # -------------------------
    ## @name Subclass Interface
    # Can be overridden by subclass
    # @{

    @log_package_handler_exception
    def _handle_added_package(self, rhs_package):
        """Called to handle if package was added, compared to the last incarnation if it's parent tree"""
        session = self.thread_local.session
        log.info("%s managed", rhs_package)
        # Will possibly create a new instance ... 
        sql_package = session.to_sql_package(rhs_package, rhs_package.stable_since())

        # NOTE: Could be a package which is currently being moved ... which just means it's not stable and will
        # seen be put under 'new' management.
        # It can be that the daemon just restarted, but the package existed already with are more useful stable_since
        # date. We will use the one from the database in that case
        if sql_package.stable_since < seconds_to_datetime(rhs_package.stable_since()):
            rhs_package.set_stable_since(datetime_to_seconds(sql_package.stable_since))
        # end handle time conversion

        # in any case, commit the changes right now (possible addition, changes)
        session.commit()


        # See if we can handle the package already
        self._handle_possibly_stable_package(rhs_package, session, sql_package)

    @log_package_handler_exception
    def _handle_removed_package(self, lhs_package):
        """Remove the package from the database"""
        log.info("%s unmanaged", lhs_package)
        session = self.thread_local.session

        # NOTE: have to handle packages which are moved accordingly ! Or which is being deleted (by us, by someone else)
        # for now, we just unmanage blindly and believe this will do just fine.
        # most importantly, here we will never allow creating a package
        try:
            sqlpkg = session.to_sql_package(lhs_package)
        except ValueError:
            # we actually assume it would exist ... this is an issue
            log.error("Couldn't get database info on package at path '%s' which should have been existing", lhs_package.root())
        else:
            # method handles case it is unmanaged already
            sqlpkg.unmanage()

            for trans in sqlpkg.transactions:
                if trans.finished_at:
                    continue
                # end ignore finished ones

                if not trans.is_queued():
                    trans.cancel()
                    trans.comment = "canceled as input package was unmanaged before transaction could be queued"
                # end only handle unqueued transactions
            # end for each transaction

            # NOTE: we are not checking for running transactions as they have to manage their state by themselves
            # and handle the case that they are removing their own packages
            session.commit()
        # end handle package
    
    @log_package_handler_exception
    def _handle_possibly_changed_package(self, lhs_package, rhs_package, modified):
        """Updates an SQL database with changes and schedules new operations to handle those packages"""
        session = self.thread_local.session

        super(SQLPackageDifferMixin, self)._handle_possibly_changed_package(lhs_package, rhs_package, modified)
        if modified:
            log.info("%s changed", rhs_package)
        else:
            log.debug("%s unchanged, stable since %i", rhs_package, rhs_package.stable_since())
        # end handle unchanged

        sql_package = None
        if modified:
            # Update the database with the stable time of the rhs package, we have changed and must mark it
            sql_package = session.to_sql_package(rhs_package, rhs_package.stable_since())
            sql_package.stable_since = seconds_to_datetime(rhs_package.stable_since())

            for trans in self._unfinished_transactions_for(session, sql_package.id):
                trans.cancel()
                trans.comment = "canceled as input package was changed before transaction was queued"
            # end for each transaction to cancel

            session.commit()
        # end pass off the package handling as it could be stable

        # We have to re-check every package to check if we can schedule a job on it
        self._handle_possibly_stable_package(rhs_package, session, sql_package)
    
    ## -- End Subclass Interface -- @}
# end class SQLPackageDifferMixin

## -- End Utilities -- @}
