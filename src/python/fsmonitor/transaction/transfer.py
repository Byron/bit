#-*-coding:utf-8-*-
"""
@package fsmonitor.transaction.transfer
@brief A simple transaction to transfer packages from a to b

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['TransferDropboxTransaction']

from time import time
import logging

from datetime import datetime

from .base import DropboxTransactionBase

from fsmonitor.sql import SQLTransactionFile
from btransaction import RsyncOperation
from butility import Path
from bkvstore import (KeyValueStoreSchema,
                             RootKey)

log = logging.getLogger('dropbox.transaction.transfer')


class TransferRsyncOperation(RsyncOperation):
    """Remembers all handled files by their full path for later"""
    __slots__ = ()

    # -------------------------
    ## @name Configuration
    # @{

    skip_empty_transfers = False
    
    ## -- End Configuration -- @}

    # -------------------------
    ## @name Interface
    # @{

    def destination_path(self):
        """@return path to directory under which all package files will be copied"""
        return self._destination_path

    def actual_destination_path(self):
        """@return root path under which the data can be found, taking into consideration the way rsync works"""
        return self._actual_destination_path
    
    ## -- End Interface -- @}



class TransferDropboxTransaction(DropboxTransactionBase):
    """An rsync based transaction to copy data around"""
    __slots__ = ()

    # -------------------------
    ## @name Constants
    # @{
    
    _plugin_name = 'transfer'


    MODE_MOVE = 'move'
    MODE_COPY = 'copy'
    MODE_SYNC = 'sync'

    valid_modes = (MODE_MOVE, MODE_COPY, MODE_SYNC)

    ## -- End Constants -- @}

    schema = KeyValueStoreSchema(RootKey, dict(mode=MODE_MOVE,              # mode of operation
                                               destination_dir=Path,        # path into which to copy the package, must exist
                                               keep_package_subdir=True,    # if True, the destination will include the relative path leading to the package
                                               subdir=Path                  # may contain substitutions like Y, M, D, H, MIN, may be empty
                                               # TODO: assure unique destination (via counter, ideally, and replaceable, even better)
                                               )
                                )


    def __init__(self, *args, **kwargs):
        """Initialize this instance with the required operations and verify configuration
        @throw ValueError if our configuration seems invalid"""
        super(TransferDropboxTransaction, self).__init__(*args, **kwargs)

        # Prepare the kvstore with data for resolving values
        now = datetime.utcnow()
        store = self._kvstore
        store.set_value('Y', now.year)
        store.set_value('M', now.month)
        store.set_value('D', now.day)
        store.set_value('H', now.hour)
        store.set_value('MIN', now.minute)

        config = self._config()

        if config.mode not in self.valid_modes:
            raise ValueError("Invalid transfer mode '%s' - must be one of %s" % (config.mode, ','.join(self.valid_modes)))
        # end check mode

        if not config.destination_dir.isdir():
            raise ValueError("Destination dropbox was not accessible: '%s'" % config.destination_dir)
        # prepare and resolve destination
        
        # handle subdir and create it if needed
        if config.subdir:
            raise NotImplementedError("implement unique identifier and subdir creation")
        # end 

        source = self._sql_instance.in_package.root()
        destination = config.destination_dir
        is_sync_mode = config.mode == self.MODE_SYNC
        if config.keep_package_subdir:
            # NOTE: rsync will duplicate our first directory unless we truncate it here
            root_relative = Path(self._package.root_relative())
            if root_relative.dirname():
                destination /= root_relative.dirname()
            # end handle modification of destination

            if is_sync_mode:
                if not source.isdir():
                    log.warn("Using copy instead of sync as it would be dangerous to use if there is no package subdirectory - source is file")
                    is_sync_mode = False
                else:
                    # In case of sync, we want to use the most destination path. This is possibly by instructing
                    # rsync to copy only the directory contents, into a destination which carries the additional
                    # base name of the source directory 
                    destination = destination / source.basename()
                    source += '/'
                # end put in sync mode safety
            # end adjust source-destination for sync mode

            # Make sure the directory exists
            if not destination.isdir():
                destination.makedirs()
            # end handle dir creation
        elif is_sync_mode:
            log.warn("Deactivating sync-mode as it is dangerous to use if keep_package_subdir is disabled")
            is_sync_mode = False
        # end handle subdir
        rsync_args = is_sync_mode and ['--delete'] or list()

        TransferRsyncOperation(self, source, destination, move=config.mode==self.MODE_MOVE, additional_rsync_args=rsync_args)
        self._sql_instance.comment = "%sing package from '%s' to '%s'" % (config.mode, source, destination)

    def _rsync_op(self):
        """@return our rsync operation"""
        return self._operations[0]

    def _add_package_files(self, session, exception = None):
        if exception:
            return
        # end don't record files on error, as rollback should have fixed it

        # Just use the recorded list - it's not worth it and cumbersome to try to use the ones 
        # we track from rsync
        super(TransferDropboxTransaction, self)._add_package_files(session, exception)

        # Create the package that the other side will have find/will have found and set it to be used
        # in our out_package slot.
        # It may or may not be under control of a dropbox on the other side
        # Also have to assure it's a real package
        rsync_destination = self._rsync_op().actual_destination_path()
        
        try:
            db = self._dropbox_finder.dropbox_by_contained_path(rsync_destination)
        except ValueError:
            db = None
        # end ignore errors


        dest_package = None
        if db and db.config_path() is not None:
            # We could try to find a matching package, but it wasn't necessarily detected yet.
            # Therefore we just get a package that matches the dropbox root and relative destination path
            for root_path in db.package_search_paths() + [db.config_path().dirname()]:
                if rsync_destination.startswith(root_path):
                    root_rela = root_path.relpathfrom(rsync_destination)
                    dest_package = session.to_sql_package((root_path, root_rela), stable_since=time())
                    dest_package.comment = "Destination of %s operation" % self._config().mode
                    break
                # end found matching root path
            # end for each package search path
        else:
            # The destination is not managed, just make up a dropbox
            dest_package = session.to_sql_package((rsync_destination, ''), stable_since=time())
            dest_package.comment = "Pseudo-package created by %s operation as destination is not a known dropbox" % self._config().mode
        # end handle have dropbox

        assert dest_package, "Should have created some sort of destination package"

        # Have to commit first, otherwise the id might be downright wrong ... 
        dest_package.commit()
        self._sql_instance.out_package = dest_package

    # -------------------------
    ## @name Interface Implementation
    # @{

    @classmethod
    def can_enqueue(cls, package, sql_package, kvstore):
        """@return always True - we have no settings that would prevent us to be enqueued, yet"""
        trs = [t for t in reversed(sql_package.in_transactions) if t.type_name == cls.plugin_name()]

        # never act on packages that were rejected in prior transactions of our type
        for trans in trs:
            if trans.is_rejected():
                log.debug("Found rejected transaction in package history - will never copy it again")
                return False
            # end ignore rejected items
        # end handle rejected

        config = kvstore.value_by_schema(cls.schema)
        # This is an odd case
        if config.mode == cls.MODE_MOVE:
            return True
        # end we can always move similar packages ... even if it's the same

        # if we are in copy mode, and if the package already has a transaction with a package matching 
        # the transaction's stable time, then the package didn't change since our last copy and 
        # we don't have to repeat it
        for trans in trs:
            if trans.error is None and \
               trans.comment and trans.comment.startswith(config.mode) and \
               trans.in_package_stable_since == sql_package.stable_since:
                log.debug("Will not rsync-copy the same package %s again as it didn't change since last time", package)
                return False
            # end prevent unnecessary copies
        # end for each transaction

        return True

    
    ## -- End Interface Implementation -- @}

# end class TransferTransaction
