#-*-coding:utf-8-*-
"""
@package fsmonitor.sql.orm
@brief The SQL schema used for the dropbox package database

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['SQLPackage', 'SQLPackageTransaction', 'SQLTransactionFile']

import logging

from sqlalchemy import (Column,
                        BigInteger,
                        String,
                        Integer,
                        DateTime,
                        ForeignKey,
                        Float,
                        Boolean,
                        Table )

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (object_session,
                            relationship)

from datetime import datetime

from butility import Path
from bit.utility import ThreadsafeCachingIDParser

log = logging.getLogger('dropbox.sql.orm')

# We use MyISAM for the files table, which will grow large
# InnoDB is used to keep track of relations between packages and operations
_table_args = dict(mysql_engine = 'MyISAM', mysql_charset = 'utf8')

class SQLBase(object):
    """Base class for all of our SQL types"""
    __slots__ = ()

    # -------------------------
    ## @name Interface
    # @{

    def commit(self):
        """Convenience method to allow each instance to commit changes.
        @return self"""
        object_session(self).commit()
        return self

    def __str__(self):
        return "%s(id=%i)" % (type(self).__name__, self.id)
    
    ## -- End Interface -- @}

# end class SQLBase

SQLBase = declarative_base(cls = SQLBase)


class SQLPackage(SQLBase):
    """A persistent version of a dropbox Package"""
    __slots__ = ()

    __tablename__  = 'package'
    __table_args__ = _table_args

    id             = Column(Integer, primary_key=True, autoincrement=True, index = True)
    host           = Column(String(128), index = True, nullable=False)
    root_path      = Column(String(512), index = True, nullable=False)  # root (dropbox) path
    package_path   = Column(String(256), index = True, nullable=False)  # package path relative to root_path

    # NOTE: all datetimes are UTC !
    managed_at     = Column(DateTime, nullable=False)
    unmanaged_at   = Column(DateTime, nullable=True)
    stable_since   = Column(DateTime, nullable=False)

    comment         = Column(String(512), nullable=True)

    # where we are source or destination
    transactions   = relationship(  'SQLPackageTransaction', 
                                    primaryjoin="(SQLPackageTransaction.in_package_id == SQLPackage.id) | (SQLPackageTransaction.out_package_id == SQLPackage.id)",
                                    lazy='select' # Load in a separate statement on first access (default)
                                    )
    
    in_transactions   = relationship(  'SQLPackageTransaction', 
                                        backref='in_package',  # create an attribute that resolves the actual package on SQLPackageTransaction
                                        foreign_keys='SQLPackageTransaction.in_package_id',
                                        lazy='select'
                                        )

    out_transactions   = relationship(  'SQLPackageTransaction', 
                                        backref='out_package',
                                        foreign_keys='SQLPackageTransaction.out_package_id',
                                        lazy='select'
                                        )

    # -------------------------
    ## @name Constants
    # @{

    FILTER_ANY = 'none'
    FILTER_MANAGED = 'managed'
    FILTER_UNMANAGED = 'unmanaged'

    FILTER_MAP = {FILTER_MANAGED : unmanaged_at == None,
                  FILTER_UNMANAGED : unmanaged_at != None}

    ## -- End Constants -- @}

    # -------------------------
    ## @name Interface
    # @{

    def is_managed(self):
        """@return True if we are currently managed. Unmanaged packages have been deleted or we lost track of 
        them through other means, like a user interfering"""
        return self.unmanaged_at is None

    def unmanage(self):
        """Set this instance's unmanaged time to now()
        @return self"""
        if self.is_managed():
            self.unmanaged_at = datetime.utcnow()
        # end handle already managed
        return self

    ## -- End Interface -- @}


    # -------------------------
    ## @name Compatibility Interface
    # @{
    
    def tree_root(self):
        return Path(self.root_path)

    def root_relative(self):
        return Path(self.package_path)

    def root(self):
        """@return our rull root-path, being a combination of tree_root and root_relative"""
        return self.tree_root() / self.root_relative()

    ## -- End Compatibility Interface -- @}

# end class SQLPackage


class SQLPackageTransaction(SQLBase):
    """A persistent representation of an operation performed on a package"""
    __slots__ = ()

    __tablename__   = 'transaction'
    __table_args__  = _table_args
    
    id              = Column(Integer, primary_key=True, autoincrement=True, index = True)
    host            = Column(String(128), index = True, nullable=False)
    type_name       = Column(String(64), index = True, nullable=False)
    in_package_id   = Column(Integer, ForeignKey(SQLPackage.id), nullable=True)
    in_package_stable_since = Column(DateTime, nullable=False)
    out_package_id  = Column(Integer, ForeignKey(SQLPackage.id), nullable=True)
    
    
    # NULL means doesn't need approval, otherwise '' indicates it is not yet approved
    approved_by_login = Column(String(128), nullable=True)

    # if NULL, the task is not queued. Otherwise it will be 0.0, and be considered queued, see is_queued()
    percent_done    = Column(Float, nullable=True)
    # NOTE: all datetime's are UTC !
    spooled_at      = Column(DateTime, nullable=False)
    started_at      = Column(DateTime, nullable=True)
    finished_at     = Column(DateTime, nullable=True)
    
    # May contain an error description - jobs that are done but have an error might be redone at some point
    error           = Column(String(512), nullable=True)
    # A comment indicating what we did, to the user. Not to be set by the use - see 'reason'
    comment         = Column(String(512), nullable=True)
    # reason for rejection or approval - will only be set by user
    reason          = Column(String(512), nullable=True)
    
    files           = relationship('SQLTransactionFile', backref='transaction')
    

    # -------------------------
    ## @name Constants
    # @{

    ## UID indicating that the transaction was automatically approved (and not approved by a real person)
    TO_BE_APPROVED_MARKER = ''
    REJECTED_MARKER = 'REJECTED'


    FILTER_PENDING = 'pending'
    FILTER_QUEUED = 'queued'
    FILTER_FINISHED = 'finished'
    FILTER_FAILED = 'failed'
    FILTER_CANCELED = 'canceled'
    FILTER_REJECTED = 'rejected'

    FILTER_MAP = {FILTER_PENDING : (approved_by_login == TO_BE_APPROVED_MARKER) & 
                                   (finished_at == None),
                  FILTER_QUEUED : (finished_at == None) & (percent_done != None),
                  FILTER_FINISHED : (finished_at != None) & (started_at != None) & (error == None),
                  FILTER_FAILED : (finished_at != None) & (started_at != None) & (error != None),
                  FILTER_CANCELED : (started_at == None) & (finished_at != None),
                  FILTER_REJECTED : approved_by_login == REJECTED_MARKER}


    AUTH_OK = 'ok'
    AUTH_WAIT = 'waiting'
    AUTH_REJECTED = 'rejected'
    AUTH_FAILURE = 'user unauthorized'
    AUTH_NOT_NEEDED = 'not needed'

    ## Actions to be applied to transactions
    # REVIEW: This should only be shared by GUI providers (e.g. cmdline, pyside), it makes no sense
    # to have it here as ORM has nothing to do with it.
    ACTION_APPROVE = 'approve'
    ACTION_REJECT = 'reject'
    ACTION_CANCEL = 'cancel'
    ACTION_LIST_FILES = 'list-files'

    ACTION_MAP = (ACTION_APPROVE, ACTION_REJECT, ACTION_CANCEL, ACTION_LIST_FILES)

    ## -- End Constants -- @}


    # -------------------------
    ## @name Class Level Utilities
    # @{

    def __str__(self):
        return "SQLPackageTransaction(id=%s,type_name=%s,in_package_id=%s)" % (self.id, self.type_name, self.in_package_id)

    ## An ID parser to help us evaluate 'id' calls, in a thread-safe version. For now ttl is hardcoded
    # but could be overridden by users using set_cache_expires_after()
    _id_parser = ThreadsafeCachingIDParser(time_to_live=60.0)
    
    ## -- End Class Level Utilities -- @}

    # -------------------------
    ## @name Interface
    # @{

    def is_queued(self):
        """@return True if we are going to be handled, and if we are not yet done"""
        return self.finished_at is None and self.percent_done is not None

    def set_queued(self):
        """Indicate that this transaction is queued.
        @return self"""
        if self.is_queued():
            return self
        # end handle extra-calls
        assert self.finished_at is None
        self.percent_done = 0.0
        return self
        
    def authentication_token(self, user_group=None):
        """@return AUTH_OK if the transaction is approved to run, or AUTH_NOT_NEEDED if no authentication is required. 
        AUTH_FAILURE is returned in case authentication failed.
        AUTH_WAIT is given if we are still waiting for approval
        @param user_group if not None, we will verify that the given uid is a member of the given group 
        (either string or gid).
        @note this method might change the indicating value if an incorrect """
        if self.approved_by_login == self.TO_BE_APPROVED_MARKER:
            return self.AUTH_WAIT
        # end waiting for approval
        if self.approved_by_login is None:
            return self.AUTH_NOT_NEEDED
        # end handle no approval required
        if self.approved_by_login == self.REJECTED_MARKER:
            return self.AUTH_REJECTED
        # end handle rejected items

        if user_group is None:
            return self.AUTH_WAIT
        # end cannot verify means we disapprove

        res = self._id_parser.parse(self.approved_by_login)
        if res is None:
            log.error("Invalid username provided for authentication: '%s'", self.approved_by_login)
            return self.AUTH_FAILURE
        # end ignore invalid names

        for gid, gname in res.groups:
            if gname == user_group or str(gid) == user_group:
                return self.AUTH_OK
            # end found matching group
        # end for each group

        log.warn("User '%s' wasn't in required group '%s', might be a hacking attempt", self.approved_by_login, user_group) 
        return self.AUTH_FAILURE

    def request_approval(self):
        """Set this instance to require approval
        @return self"""
        self.approved_by_login = self.TO_BE_APPROVED_MARKER
        return self

    def reject_approval(self):
        """If this transaction is waiting for approval (and is not auto-approved), it will be marked rejected.
        @note it is an error to reject an approved package. It's safe to call it multiple times
        @return self"""
        if self.approved_by_login == self.REJECTED_MARKER:
            return self
        # end skip if we are rejected

        assert self.approved_by_login == self.TO_BE_APPROVED_MARKER, "Can't reject a transaction that is not for approval"
        self.approved_by_login = self.REJECTED_MARKER
        return self

    def is_rejected(self):
        """@return True if we were rejected, and thus are disapproved"""
        return self.approved_by_login == self.REJECTED_MARKER

    def is_cancelled(self):
        """@return True if this transaction was canceled"""
        return self.started_at is None and self.finished_at is not None

    def cancel(self):
        """Set this transaction to be canceled. It's safe to cancel multiple times.
        @return self"""
        if self.finished_at is None:
            self.finished_at = datetime.utcnow()
        # end handle cancellation
        return self
        
    ## -- End Interface -- @}

# end class SQLPackageTransaction


class SQLTransactionFile(SQLBase):
    """A file that was handledby an SQLPackageTransaction in one way or another"""
    
    __slots__ = ()

    __tablename__  = 'transaction_file'
    __table_args__ = _table_args
    
    id             = Column(Integer, primary_key=True, autoincrement=True)

    # id of the transaction that affected us
    transaction_id = Column(Integer, ForeignKey(SQLPackageTransaction.id), nullable=False, index=True)
    
    path           = Column(String(2000), nullable=False)
    size           = Column(BigInteger, nullable=False)
    uid            = Column('uid', Integer, nullable=False)
    gid            = Column('gid', Integer, nullable=False)
    mode           = Column('mode', Integer, nullable=False)

# end class SQLTransactionFile
