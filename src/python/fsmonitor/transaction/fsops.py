#-*-coding:utf-8-*-
"""
@package fsmonitor.transaction.transfer
@brief A simple transaction to transfer packages from a to b

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DeleteDropboxTransaction']

from time import (time,
                  gmtime,
                  strftime,
                  timezone)
import logging

from .base import DropboxTransactionBase

from bkvstore import (KeyValueStoreSchema,
                      FrequencyStringAsSeconds,
                      RootKey)

from butility import Path

from btransaction.operations.fsops import (DeleteOperation,
                                           MoveFSItemOperation,
                                           CreateFSItemOperation)

log = logging.getLogger('dropbox.transaction.transfer')



class DeleteDropboxTransaction(DropboxTransactionBase):
    """A simple delete operation for dropboxes"""
    __slots__ = ()

    # -------------------------
    ## @name Constants
    # @{
    
    _plugin_name = 'delete'

    ## -- End Constants -- @}

    schema = KeyValueStoreSchema(RootKey, dict(after_being_stable_for=FrequencyStringAsSeconds('1d')))                  # mode of operation


    def __init__(self, *args, **kwargs):
        """Initialize this instance with the required operations and verify configuration
        @throw ValueError if our configuration seems invalid"""
        super(DeleteDropboxTransaction, self).__init__(*args, **kwargs)

        DeleteOperation(self, self._sql_instance.in_package.root())
        self._sql_instance.comment = "Deleting package as it was stable for more than %s" % self._config().after_being_stable_for.frequency

    # -------------------------
    ## @name Interface Implementation
    # @{

    @classmethod
    def can_enqueue(cls, package, sql_package, kvstore):
        """@return True if the package surpassed it's time to live"""
        return (time() - package.stable_since()) >= kvstore.value_by_schema(cls.schema).after_being_stable_for.seconds
    
    ## -- End Interface Implementation -- @}
# end class TransferTransaction


class MoveDropboxTransaction(DropboxTransactionBase):
    """A transaction which moves a package in order to normalize it.

    Please note that the move operation needs to be done on a single file system ! Otherwise, use the 
    TransferDropboxTransaction which is rsync based, but doesn't allow to rename the destination.

    Basic criteria are various dates available in the stat() structure, like ctime or mtime.
    Using a strptime based template system, it's easy to rename/move a package, which can be a file or directory.

    NOTE
    ====

    For now the move operation will always be performed, which means that the destination must be outside of the 
    dropbox search path to prevent an infinite move loop !
    """
    __slots__ = ()

    # -------------------------
    ## @name Constants
    # @{

    _plugin_name = 'move'

    ## A list of time fields we may use
    valid_times = ('ctime', 'mtime', 'atime')
    
    ## -- End Constants -- @}

    schema = KeyValueStoreSchema(   RootKey, 
                                    dict(date_field="mtime", # the stat() field to use, we prefix 'st_'
                                         destination=str     # absolute or relative path, may contain strptime wildcards
                                         ))

    def __init__(self, *args, **kwargs):
        """Initialize the underlying move operation"""
        super(MoveDropboxTransaction, self).__init__(*args, **kwargs)

        destination = self._normalized_destination(self._package, self._config())

        # Assure we create the parent directory
        CreateFSItemOperation(self, destination.dirname())
        MoveFSItemOperation(self, self._package.root(), destination)

    # -------------------------
    ## @name Utilities
    # @{

    @classmethod
    def _normalized_destination(cls, package, config):
        """@return an absolute path with all strp based items replaced if applicable"""
        stat = package.root().stat()
        assert config.date_field in cls.valid_times, "Invalid date field, must be one of %s" % (', '.join(cls.valid_times))
        assert config.destination, "'destination' field must be set"

        time_tuple = gmtime(getattr(stat, 'st_%s' % config.date_field) - timezone)

        destination_path = Path(strftime(config.destination, time_tuple))
        if not destination_path.isabs():
            destination_path = package.root() / destination_path
        # end make absolute

        return destination_path

    @classmethod
    def can_enqueue(cls, package, sql_package, kvstore):
        """@return always True"""
        return True

    ## -- End Utilities -- @}

# end class MoveDropboxTransaction
