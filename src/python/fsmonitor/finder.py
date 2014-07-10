#-*-coding:utf-8-*-
"""
@package dropbox.finder
@brief Implements finder utility for dropboxes

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DropboxFinder']

import os
from fnmatch import fnmatch

import tx
from butility import Path

from .base import Dropbox

log = service(tx.ILog).new('dropbox.finder')


class DropboxFinder(object):
    """Keeps a list of dropboxes found on hardisk based on a glob by default and assuming that dropboxes 
    are never contained in other dropboxes.

    Each time update is run, it will inform you about added, removed and changed dropboxes (regarding their config file)
    """
    # It's sad, but slots don't work with multi-inheritance. So subclasses can't really do what they want because
    # of this ... its a special case, and bad design that subclasses make the base class change.
    # However, alternatives are uglier, and the root cause of the problem is that bloody python doesn't deal with 
    # slots correctly in multi-inheritance cases for some weird and unknown reason.
    _slots_ = ('paths', 'max_depth', 'config_file_glob', 
                 'dropboxes'    # dict: config_file_path -> (stat, dropbox)
                 )

    # -------------------------
    ## @name Configuration
    # @{

    ## Type to use when instantiating a dropbox
    DropboxType = Dropbox
    
    ## -- End Configuration -- @}

    
    def __init__(self, paths, max_depth=1, config_file_glob='.dropbox.yaml'):
        """Initialize this instance
        @param paths list of Path instances to be searched
        @param max_depth recursive depth of the search. 1 is needed to just check the search path directory itself
        @param config_file_glob an fnmatch compatible string to find dropbox configuration files"""
        self.paths = paths
        self.max_depth = max_depth
        self.config_file_glob = config_file_glob
        self.dropboxes = dict()

    def __getitem__(self, key):
        """@return plain dropbox at key"""
        return self.dropboxes[key][1]

    # -------------------------
    ## @name Interface
    # @{

    def update(self, known_only=False):
        """Update our set of dropboxes to represent the latest state on disk
        @param known_only if True will not actually search for new dropboxes, but only check if existing dropboxes
        have had their configuration changed or were removed
        @return self"""
        def update_stat(dbpath, stat, db):
            try:
                new_stat = dbpath.stat()
            except OSError:
                del self.dropboxes[dbpath]
                self._dropbox_removed(db)
            else:
                if new_stat.st_size != stat.st_size or new_stat.st_mtime != stat.st_mtime:
                    self.dropboxes[dbpath] = (new_stat, db)
                    self._dropbox_changed(stat, new_stat, db)
                # end handle change
            # end handle dropbox doesn't exist
        # end utility to test stat

        if known_only:
            for dbpath, (stat, db) in self.dropboxes.iteritems():
                update_stat(dbpath, stat, db)
            # end for each stat, db
        else:
            seen_paths = set()
            for search_base in self.paths:
                if search_base.endswith(os.path.sep):
                    search_base = Path(search_base[:-1])
                # end assure we don't end with slash
                if not search_base.isdir():
                    log.warn("Skipping unaccessible search base at '%s'", search_base)
                    continue
                # end 
                log.debug("Searching for dropboxes under '%s' (depth=%i, glob='%s')", 
                                                search_base, self.max_depth, self.config_file_glob)

                num_dropboxes = 0  # Amount of dropboxes found for this search base
                for root, dirs, files in os.walk(search_base):
                    if root[len(search_base):].count(os.path.sep) == self.max_depth - 1:
                        del dirs[:]
                    # end handle aborting recursion

                    for match in (f for f in files if fnmatch(f, self.config_file_glob)):
                        dbpath = Path(root) / match
                        seen_paths.add(dbpath)
                        num_dropboxes += 1
                        if dbpath in self.dropboxes:
                            # check for change
                            stat, db = self.dropboxes[dbpath]
                            update_stat(dbpath, stat, db)
                        else:
                            # handle new dropbox
                            try:
                                stat = dbpath.stat()
                            except OSError:
                                log.error("Couldn't stat dropbox configuration at '%s' even though it was found during search", dbpath)
                            else:
                                dropbox = self.DropboxType(dbpath)
                                self.dropboxes[dbpath] = (stat, dropbox)
                                self._dropbox_added(stat, dropbox)
                            # end handle inaccessible config file (invalid ACL ?)
                        # end handle update or new
                    # end handle each match
                # end for each root, dir, files
                if num_dropboxes == 0:
                    log.warn("Didn't find a single dropbox in search base '%s'", search_base)
                # end info log
            # end for each search_base

            # Check for deleted
            for deleted_db_path in (set(self.dropboxes.keys()) - seen_paths):
                stat, db = self.dropboxes[deleted_db_path]
                del self.dropboxes[deleted_db_path]
                self._dropbox_removed(stat, db)
            # end for each deleted
        # end handle known only
        return self

    def iter_dropboxes(self):
        """@return iterator over all contained dropbox instances"""
        return (db for (stat, db) in self.dropboxes.itervalues())

    def dropbox_by_contained_path(self, path):
        """@return a dropbox that contains the given path, but not by the configuration path, but by the tree
        search path, as it is more precise.
        @param path usually the root path of a package
        @throws ValueError if there is no dropbox matching the path. Could happen if the dropbox is deleted in the
        moment the package is handled by another thread."""
        for db in self.iter_dropboxes():
            for search_path in db.package_search_paths():
                if path.startswith(search_path):
                    return db
                # end return db
            # end for each tree search path
        # end for each dropbox
        raise ValueError("no dropbox found that would contain path '%s'" % path)
    
    ## -- End Interface -- @}


    # -------------------------
    ## @name Subclass Interface
    # @{

    def _dropbox_added(self, stat, dropbox):
        """Called whenever a new dropbox was added to our data structure.
        When called, it is already part of our dict
        @param stat structure of dropbox's configuration file
        @param dropbox new Dropbox instance, without queried trees"""
        log.info("Found %s" % dropbox)

    def _dropbox_removed(self, stat, dropbox):
        """Called as the given dropbox instance was removed from our datastructure, as it's configuration
        file doesn't exist anymore
        @param stat last known stat structure of the config file of the dropbox
        @param dropbox dropbox instance that was removed"""
        log.info("Removed %s - configuration was removed" % dropbox)

    def _dropbox_changed(self, prev_stat, new_stat, dropbox):
        """Called to handle when a dropboxes configuration file was modified, which should cause the dropbox
        instance to update too
        @param prev_stat structure previously taken
        @param new_stat structure newly taken"""
        log.info("Reloading configuration of %s", dropbox)
        dropbox.clear_configuration_cache()
        
    
    ## -- End Subclass Interface -- @}


# end class DropboxFinder
