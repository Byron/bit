#-*-coding:utf-8-*-
"""
@package dropbox.transaction.base
@brief Base implementation for dropbox transactions

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DropboxTransactionBase']

from time import time
from socket import gethostname
from datetime import datetime

from tx.core.kvstore import ChangeTrackingKeyValueStoreModifier
from tx.processing.transaction import (Transaction,
                                       StoringProgressIndicator)
                                       

from dropbox.sql import (with_threadlocal_session,
                         SQLPackageTransaction,
                         SQLTransactionFile)
from sqlalchemy.orm import object_session
from weakref import proxy


class DropboxTransactionProgressIndicatorMixin(StoringProgressIndicator):
    """A mixin for DropboxTransactionBase instances to update our sql instance as needed"""

    # -------------------------
    ## @name Constants
    # @{

    ## Amount of seconds we should wait before updating the progress and message within the database
    UPDATE_DB_INTERVAL = 1.0
    
    ## -- End Constants -- @}

    def __init__(self, *args, **kwargs):
        """initialize our variables"""
        super(DropboxTransactionProgressIndicatorMixin, self).__init__(*args, **kwargs)
        self._last_message_update = time()

    # -------------------------
    ## @name Superclass Overrides
    # @{
    
    def begin(self):
        """Make sure we are updating our sql instance accordingly.
        @note handles rollbacks"""
        if not self.is_rolling_back():
            self._sql_instance.started_at = datetime.utcnow()
            self._sql_instance.commit()
        # end setup start date

    def end(self):
        """Update our sql information. We count the time it takes to rollback in, therefore we will just
        set the finished_at value whenever we are called"""
        self._sql_instance.finished_at = datetime.utcnow()
        self._sql_instance.commit()

    def set(self, *args, **kwargs):
        """Everytime we are set, we also want to consider updating the progress within the database"""
        res = super(DropboxTransactionProgressIndicatorMixin, self).set(*args, **kwargs)
        ct = time()
        if ct >= self._last_message_update + self.UPDATE_DB_INTERVAL:
            self._sql_instance.comment = self._message
            self._sql_instance.percent_done = self.get()
            self._sql_instance.commit()
            self._last_message_update = ct
        # end update database
        return res

    ## -- End Superclass Overrides -- @}

        

# end class DropboxTransactionProgressIndicatorMixin


class DropboxTransactionBase(Transaction, DropboxTransactionProgressIndicatorMixin, Plugin):
    """A dropbox transaction is just a transaction, but will work with an SQL persistence layer of itself and allows
    to take custom options as defined in a schema.

    Our configuration comes from either the daemon for global settings, but may be overridden by any dropbox 
    whose package we are handling.

    Some methods are overridden to automatically handle and update the sql transaction associated with us.
    That way, status updates and results are made persistent.
    """
    _slots_ = ('_kvstore',
               '_sql_instance_id',
               '_sql_instance',
               '_package',  # the package we will handle
               '_tree',     # the tree containing the package files (and all stat info)
               '_dropbox_finder', # a DropboxFinder instance that can be used to find destination dropboxes
               '_url', # url for instantiating new sessions
               '__weakref__')

    # -------------------------
    ## @name Configuration
    # @{

    ## KeyValueStoreSchema defining configuration values of this implementation
    # To be set in subclass
    # NOTE: normally your keys would be in the root namespace, there is no need to use any sub-keys
    schema = None

    ## In our case, the plugin name is used as an identifier for the operation itself.
    # It may be employed to access configuration in a kvstore, even though we do not impose any constraints here.
    _plugin_name = None

    ## -- End Configuration -- @}

    def __init__(self, *args, **kwargs):
        """Initialize this instance, add a kvstore to provide configuration
        @param kvstore to use as source for configuration
        @param sql_instance transaction instance which will be used for persisting our results. It will only be 
        used from within this thread
        @param package instance with a valid tree
        @param dropbox_finder a DropboxFinder instance 
        @note subclasses should override this method and initialize their operations based on the configuration
        in the kvstore, which must be queried using self.schema (kvstore.value_by_schema(...))
        @note you are only instantiated if you are going to be queued (see can_enqueue() for more information)"""
        self._kvstore = kwargs.pop('kvstore')
        # We keep the instance, but will regenerate it within the method that is supposed to be threaded
        self._sql_instance = kwargs.pop('sql_instance')
        self._sql_instance_id = self._sql_instance.id
        self._url = str(object_session(self._sql_instance).connection().engine.url)
        self._package = kwargs.pop('package')
        self._dropbox_finder = kwargs.pop('dropbox_finder')
        self._tree = self._package.tree()       # keep a strong reference to the original tree

        assert 'progress' not in kwargs, "Cannot set progress, as we are completely overriding this functionality"

        # Set ourselves up as progress handler
        kwargs['progress'] = proxy(self)
        super(DropboxTransactionBase, self).__init__(*args, **kwargs)
        DropboxTransactionProgressIndicatorMixin.__init__(self)

    def __str__(self):
        return "%s(type_name='%s', id=%i)" % (type(self).__name__, self.plugin_name(), self._sql_instance_id)

    def _config(self):
        """@return the configuration based on our schema
        @note values will be resolved, which enables substitution of strings with data in the kvstore"""
        return self._kvstore.value_by_schema(self.schema, resolve=True)

    # -------------------------
    ## @name Subclass Interface
    # @{

    def _completed(self, session, exception = None):
        """Called once our transaction was completed.
        @param session package session in case you want to update the database. It is NOT necessary to commit
        as it will be done by the base implementation.
        @param exception indicating if there was an exception or not. Use this information to determine what 
        to set in the database.
        @note you may freely use our _sql_instance member, usually only needed to set the comment though
        @note subclasses must call their superclass accordingly"""
        if exception:
            self._sql_instance.error = str(exception)
        else:
            self._sql_instance.percent_done = 100.0
        # end handle exception

        self._add_package_files(session, exception)

        # no matter whether it was an error or not, we shall be finished
        self._sql_instance.finished_at = datetime.utcnow()


    def _add_package_files(self, session, exception = None):
        """By default, we are adding all package files to the list of handled files in the database.
        We will add the files even if there was an error, for completeness
        @note override this method if you need different behaviour"""
        tree_root = self._tree.root_path()
        for rela_path, stat in self._package.entries():
            abs_path = tree_root / rela_path
            sql_file = SQLTransactionFile(transaction = self._sql_instance, 
                                          path = str(abs_path),
                                          size = stat.st_size,
                                          uid  = stat.st_uid,
                                          gid  = stat.st_gid,
                                          mode = stat.st_mode)
            session.add(sql_file)
        # end for each file
        

    
    ## -- End Subclass Interface -- @}

    # -------------------------
    ## @name Interface
    # @{

    @classmethod
    def can_enqueue(cls, package, sql_package, kvstore):
        """@return True if this transaction is ready to be queued, solely based on our own configuration.
        @param package a DropboxPackage instance 
        @param kvstore suitable for use with our schema, and what you would get when instantiated
        Use the information provided as argument to make that decision"""
        raise NotImplementedError("To be implemented in subclass")        

    def sql_instance(self):
        """@return our incarnation within the sql database.
        Will be None after we have run"""
        return self._sql_instance
    
    ## -- End Interface -- @}


    # -------------------------
    ## @name Transaction Overrides
    # @{

    @with_threadlocal_session
    def apply(self, session):
        """Used to determine if the operation failed and records how it failed"""
        self._sql_instance = session.query(SQLPackageTransaction).filter(SQLPackageTransaction.id == self._sql_instance_id)[0]

        res = super(DropboxTransactionBase, self).apply()

        try:
            self._completed(session, self.exception())
        except Exception, err:
            self.log.error("Failed when trying to complete the transaction - will not rollback, it's most likely a bug", exc_info=True)
            # conserve the error without loosing the other one
            self._sql_instance.error = (self._sql_instance.error and  (self._sql_instance.error + "|") or "") + str(err)
        # end handle our own failure
        
        self._sql_instance.commit()
        self._sql_instance = None

        return res
    
    ## -- End Transaction Overrides -- @}
    

# end class DropboxTransactionBase
