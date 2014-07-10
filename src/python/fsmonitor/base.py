#-*-coding:utf-8-*-
"""
@package dropbox.base
@brief Basic dropbox types

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['Dropbox']

import time
from itertools import chain

import tx
from bit.utility import KVFrequencyStringAsSeconds
from tx.core.kvstore import (KeyValueStoreProvider,
                             KeyValueStoreSchema,
                             StringList,
                             AnyKey,
                             RootKey,
                             YAMLStreamSerializer)
from tx.core.environ.settings import (PersistentSettingsEnvironmentStackContextClient,
                                      PersistentSettings)
from butility import Path

from .tree import TreeRoot

log = service(tx.ILog).new('dropbox')


class _PersistentYAMLSettings(PersistentSettings):
    """Just used to override the StreamSerializer to become yaml"""
    StreamSerializerType = YAMLStreamSerializer

#end class _PersistentYAMLSettings


class Dropbox(PersistentSettingsEnvironmentStackContextClient):
    """A type representing a dropbox.

    Each dropbox has configuration, which is used to determine the expected kind of behaviour.
    Dropboxes serve as inventory for their contents, and are able to efficiently track changes.

    They group directories with contents into so called Packages, which may persist in a database.

    As a dropbox doesn't do much itself, its primarily a container for configuration.
    """
    __slots__ = ('_config_file_path',  # path from which to pull our configuration
                 '_trees',             # lists of trees based on search paths in our configuration
                )

    ## Be sure we use standard yaml for readability
    SettingsType = _PersistentYAMLSettings

    TRANSACTIONS_KEY = 'transactions'

    # Our basic configuration - it's not based on the global kvstore
    _schema = KeyValueStoreSchema(RootKey, {    'package' : dict(stable_after=KVFrequencyStringAsSeconds('60s'),   # amount of time after which a package is stable, and allow operations to run on them.
                                                             search_paths=StringList),     # a list of relative or absolute paths to directories to inventory
                                                'auto_approve' : StringList,               # Names of transactions which are automatically approved to be queued
                                                'one_package_per_file' : False,            # If True, there will be one package per file, otherwise packages usually are directories
                                                'update_packages_every' : KVFrequencyStringAsSeconds(), # An optional override for how often our packages should be updated
                                                })


    def __init__(self, configuration):
        """Initialize this instance
        @param configuration configuration file to use to provide us with additional information.
        Can also be a ChangeTrackingKeyValueStoreProvider if you don't want file-based configuration. Useful for testing"""
        self._config_file_path = None
        self._trees = None
        if isinstance(configuration, KeyValueStoreProvider):
            self._settings_kvstore = configuration
        else:
            self._config_file_path = configuration
        # end handle input type

    def __str__(self):
        return "Dropbox('%s')" % self.config_path()

    def _settings_path(self):
        """NOTE: Used by our base class"""
        return self._config_file_path

    def _new_trees(self):
        """Obtain a new sample of TreeRoot instances
        @note this is an expensive operation as TreeRoot instances parse their data upon creation
        @return the obtained list of TreeRoots"""
        trees = list()
        one_package_per_file = self.settings().one_package_per_file
        for search_path in self.package_search_paths():
            if search_path.isdir():
                trees.append(TreeRoot(search_path, one_package_per_file))
            else:
                log.warning("Ignored inaccessible dropbox path at '%s' - please check configuration file at '%s'", search_path, self._settings_path())
            # end handle search path
        # end for each path
        return trees


    # -------------------------
    ## @name Subclass Overrides
    # @{

    def settings_id(self):
        raise NotImplementedError("Implementation not required")

    def trees(self):
        """@return a list of TreeRoot instances that represent our package search paths (see configuration)"""
        if self._trees is None:
            self._trees = self._new_trees()
        # end sample auto-update
        return self._trees

    def iter_packages(self):
        """@return an iterator over all packages of all of our trees, in order"""
        for tree in self.trees():
            for package in tree.iter_packages():
                yield package
            # end for each package in tree
        # end for each tree

    def config_path(self):
        """@return Path at which to find our configuration file. May be None if we were initialized 
        with a KVStore instance"""
        return self._settings_path()

    def package_search_paths(self):
        """@return list of absolute package search paths, under which we will check for packages and changes thereof"""
        out = list()
        for search_path in self.settings().package.search_paths:
            search_path = Path(search_path)
            if not search_path.isabs():
                assert self._config_file_path is not None, "Require a configuration file path for relative search paths"
                search_path = self._config_file_path.dirname() / search_path
            # end make absolute path
            out.append(search_path)
        # end for each possibly relative search path
        return out

    def diff_tree_sample_packages(self, differ):
        """When called, a new tree sample is taken for comparison with the previous incarnation of our trees.
        The differ instance is used to perform the comparison of all packages contained in both tree samples.
        The new trees sample will be stored within this instance
        @param differ a PackageDiffer instance
        @return value returned by differ.diff()
        @note it is absolutely required that TreeRoots are not a subset of each others, and thus that packages
        are unique among TreeRoots of each sample time
        """
        if self._trees is None:
            # on first call, we don't have anything to compare to, therefore everything seems to have been added
            # It makes no sense to take two samples at the same time ... 
            new_trees = self.trees()
            old_trees = list()
        else:
            old_trees = self.trees()
            self._trees = new_trees = self._new_trees()
        # end only one sample special handling
        return differ.diff(chain(*list(t.iter_packages() for t in old_trees)), 
                           chain(*list(t.iter_packages() for t in new_trees)))

    def clear_configuration_cache(self, configuration=None):
        """Clear our configuration to re-evaluate our trees (and packages) next time we are queried
        @param configuration if not None, it may be a KeyValueStoreProvider to replace our configuration.
        @return self"""
        if isinstance(configuration, KeyValueStoreProvider):
            self._settings_kvstore = configuration
        else:
            assert self._config_file_path, "Dropbox needs to be initialized with a config file path if configuration cache is to be cleared"
            for attr in ('_settings_data', '_settings_kvstore'):
                try:
                    delattr(self, attr)
                except AttributeError:
                    pass
                # end handle attribute deletion
            # end for each attr to delete
        # end handle type of configuration value
        return self

    def last_tree_sample_time(self):
        """@return time at which our last tree started sampling it's meta-data, or None if there was no sample yet"""
        if not self._trees:
            return None
        # end handle no sample taken yet
        return self._trees[-1].sample_time()

    ## -- End Subclass Overrides -- @}
    
# end class Dropbox
