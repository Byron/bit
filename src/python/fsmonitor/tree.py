#-*-coding:utf-8-*-
"""
@package dropbox.tree
@brief a simple utility type representing the state of a tree

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['TreeRoot', 'Package', 'PackageDiffer']

import os
from stat import S_ISDIR
from time import time
from weakref import ref

from butility import Path


class TreeRoot(dict):
    """A python representation of a directory tree

    It keeps a tree-like structure in a simple dict, where each tree entry is associated with a tuple of meta-data.
    This property makes it comparable and easily diffable.
    As the value of a directory entry is another dict with items, we don't have stat information on a dictionary itself.
    File entries will contain the stat structure obtained by os.stat

    Additionally, a tree is able to find package root paths, and allows simplified access to sub-paths using a path 
    separator right away.

    Packages are items underneath which there is at least one file. A package starts at the path which actually
    contains a file.
    A package may be a file.

    A package is a simple helper to keep track of everything below it
    """
    __slots__ = ('_root_path', 
                 '_sample_time',
                 '_packages',
                 '_one_package_per_file',
                 '__weakref__')

    # -------------------------
    ## @name Constants
    # @{

    ## we assume tokens to be separated with this separator
    TOKEN_SEPARATOR = '/'
    
    ## -- End Constants -- @}


    def __new__(cls, root_path, *args, **kwargs):
        """Just required to allow custom constructor"""
        return dict.__new__(cls)
        
    def __init__(self, root_path, one_package_per_file=False):
        """Initialize this instance from the given root path and parse all information contained in the directory
        @param root_path tx.Path instance, pointing to an existing directory
        @param one_package_per_file if True, every file will be a package."""
        self._root_path = Path(root_path)
        self._packages = None
        self._one_package_per_file = one_package_per_file
        self._init_contents()

    # -------------------------
    ## @name Utilities
    # @{

    def _init_contents(self):
        """Initialize our data by parsing it from disk.
        @note can only be called once as we have to be empty"""
        assert len(self) == 0, "Need to be empty, can only be called once"
        assert self._root_path.isdir(), "Root path must be directory"

        # Main recursion helper, depth first
        # root_dict - dict to put information into
        # root_path - path to directory to analyze
        ls = os.listdir
        stat = os.stat
        join = os.path.join

        def recurse_dir(root_dict, root_path):
            for entry in ls(root_path):
                # entry is just the name
                absentry = join(root_path, entry)
                try:
                    einfo = stat(absentry)
                except OSError:
                    continue
                # end ignore files deleted under our nose

                if S_ISDIR(einfo.st_mode):
                    root_dict[entry] = recurse_dir(dict(), absentry)
                else:
                    root_dict[entry] = einfo
                # end 
            # end for each entry
            return root_dict
        # end recursion helper
        recurse_dir(self, self._root_path)

        # once we are done with the sample, we set the sample time. Otherwise packages might be considered 
        # stable just because the it took us many seconds until the sample was taken
        self._sample_time = time()

    ## -- End Utilities -- @}


    # -------------------------
    ## @name Superclass Overrides
    # @{

    def __str__(self):
        return 'TreeRoot("%s")' % self.root_path()

    def __getitem__(self, name):
        """If name contains a token separator, recurse into ourselves to return the result
        @raise KeyError if there no entry at the given path"""
        for token in name.split(self.TOKEN_SEPARATOR):
            self = dict.__getitem__(self, token)
        # end handle recursion

        return self

    ## -- End Superclass Overrides -- @}

    # -------------------------
    ## @name Interface
    # @{

    def root_path(self):
        """@return our root path"""
        return self._root_path

    def sample_time(self):
        """@return time (as seconds since epoch) at which our snapshot was taken.
        @note basically the time of our instantiation"""
        return self._sample_time

    def entries(self, root_relative):
        """@return list of all entries in ourselves, depth first, files only, as tuple of (rela_path, stat)
        @param root_relative relative path into our dict - can contain path separators"""
        out = list()
        def recurse_dir(root_item, root_path):
            if isinstance(root_item, dict):
                for key in root_item:
                    recurse_dir(root_item[key], root_path + self.TOKEN_SEPARATOR + key)
                # end for each key in ourselves
            else:
                out.append((root_path, root_item))
            # end handle dict/non-dict
        # end recursion helper
        recurse_dir(self[root_relative], root_relative)
        return out

    def iter_packages(self):
        """@return an iterator yielding all Package instances found in this tree
        @note we are caching the package just because this will allow them to carry on their own stable_since 
        date. Otherwise it wouldn't be a problem at all to obtain packages on demand
        """
        # We build a cache only once actually
        if self._packages is None:
            self._packages = list()
            # Recurse into our structure and find indication for packages.
            # Abort recursion once criteria are met, and handle files specifically underneath 
            # our root

            # filter a dict's contents into files and dir tuples
            def files_and_dirs(d):
                files = list()
                dirs = list()
                for name, entry in d.items():
                    if isinstance(entry, dict):
                        dirs.append((name, entry))
                    else:
                        files.append((name, entry))
                    # end handle entry type
                # end for each name, entry
                return files, dirs
            # end files and dirs

            join = os.path.join
            files, dirs = files_and_dirs(self)

            for name, info in files:
                self._packages.append(Package(self, name))
            # end for each file underneath
            
            # for each directory, enter standard recursion, with standard rules, and yield items
            def recurse(dir_dict, subdir_relative):
                # if there is a single file, it's a package
                files, dirs = files_and_dirs(dir_dict)
                if files:
                    if self._one_package_per_file:
                        for file in files:
                            self._packages.append(Package(self, subdir_relative + os.path.sep + file[0]))    
                        # end for each file
                    else:
                        self._packages.append(Package(self, subdir_relative))
                    # end handle package instantiation
                else:
                    for name, dir_dict in dirs:
                        recurse(dir_dict, join(subdir_relative, name))
                    # end for each directory to traverse
                # end handle recursion
            # end recursion helper

            for name, dir_dict in dirs:
                recurse(dir_dict, name)
            # end for each name, dir_dict
        # end build cache

        return iter(self._packages)

    ## -- End Interface -- @}

# end class TreeRoot


class PackageDiffer(object):
    """Generates a diff-index with added, removed and changed packages.

    In addition to that, it will remove no other
    @note as our structure is dead-simple, we are just implementing it ourselves
    """
    __slots__ = ()

    def diff(self, lhs, rhs):
        """Diff the given list of packages using a standard two-way diff algorithm.
        @param lhs an iterator yielding package instances, like TreeRoot.iter_packages(), all from one tree !
        @param rhs as lhs
        @note we don't produce a diff-index, but require a subclass to implement handlers for addition, removal, change
        @return result of iterating rhs
        """
        lhsp = dict((p.root(), p) for p in lhs)
        rhsp = dict((p.root(), p) for p in rhs)

        lhsk = set(lhsp.keys())
        rhsk = set(rhsp.keys())

        # RHS ADDED
        for key in (rhsk - lhsk):
            self._handle_added_package(rhsp[key])
        # end

        # RHS DELETED
        for key in (lhsk - rhsk):
            self._handle_removed_package(lhsp[key])
        # end

        # RHS MODIFIED
        for key in (lhsk & rhsk):
            self._handle_possibly_changed_package(lhsp[key], rhsp[key], not (lhsp[key] == rhsp[key]))
        # end

        return rhsp.values()


    # -------------------------
    ## @name Subclass Interface
    # Can be overridden by subclass
    # @{

    def _handle_added_package(self, rhs_package):
        """Called to handle if package was added, compared to the last incarnation if it's parent tree"""

    def _handle_removed_package(self, lhs_package):
        """Called to handle if the given lhs package is not existing in the rhs tree anymore"""
        
    def _handle_possibly_changed_package(self, lhs_package, rhs_package, modified):
        """Called to handle if the lhs and rhs package changed in one way or another.
        Default implementation will set the rhs_package 
        @param lhs_package package part of the lhs tree
        @param rhs_package package as part of the rhs tree
        @param modified if True, it indicates that lhs != rhs"""
        if not modified:
            # inherit last modification, which is the last known stable date. It might be inherited as well,
            # and thus propagates itself
            rhs_package.set_stable_since(lhs_package.stable_since())
        # end handle modification
    
    ## -- End Subclass Interface -- @}
# end class PackageDiffer


class Package(object):
    """A package as contained in our tree.

    It is more like a pointer to a bundle of files in a particular tree.

    Packages know the tree they live in and may thus compute a relative root path, that is their path relative
    to the root of their tree root instance.
    """
    __slots__ = ('_root_relative', 
                 '_tree',
                 '_changed_at')

    def __init__(self, tree, root_relative):
        """Initialize this instance with a little bit of contextual information
        @param root_relative tree-root relative path as string
        @param tree TreeRoot instance that contains us"""
        self._root_relative = root_relative
        self._tree = ref(tree)
        self._changed_at = tree.sample_time()

    def __repr__(self):
        return 'Package(%s, "%s")' % (self.tree(), self.root_relative())

    def __lt__(self, rhs):
        """Use our relative root for comparison"""
        return self.root_relative() < rhs.root_relative()

    def __eq__(self, rhs):
        """@return True if we have the same contents as rhs, based on our meta-data.
        @note handles incompatible types of rhs"""
        if not isinstance(rhs, Package):
            return False
        # end handle different type
        return self.tree()[self.root_relative()] == rhs.tree()[rhs.root_relative()]

    def __ne__(self, rhs):
        """@return opposite of =="""
        return not (self == rhs)

    # -------------------------
    ## @name Interface
    # @{

    def set_tree(self, tree):
        """Set ourselves to be owned by the given tree
        @throw ValueError if we are not contained in the given tree
        @return self"""
        try:
            package_data = tree[self.root_relative()]
        except KeyError:
            raise ValueError("Tree doesn't contain this package")
        # end validate input

        # If this package changed in the new tree, update the change_at time accordingly
        if package_data != self.tree()[self.root_relative()]:
            self._changed_at = tree.sample_time()
        # end handle changed time
        self._tree = ref(tree)
        return self

    def set_stable_since(self, time):
        """Set this instance to be stable since the given time
        @param time in seconds since epoch
        @note useful to reset a newly found package to a sample time known by the same unchanged package, 
        sampled at an earlier time
        @return self"""
        self._changed_at = time
        return self

    def tree(self):
        """@return the TreeRoot instance we are belonging to"""
        tree = self._tree()
        assert tree is not None, "Parent tree went out of scope"
        return tree

    def entries(self):
        """@return list of entries in our package. See TreeRoot.entries()"""
        return self.tree().entries(self.root_relative())
        
    def root(self):
        """@return absolute root path at which this package is located.
        Can point to a file or a directory"""
        return self.tree_root() / self.root_relative()

    def tree_root(self):
        """@return root path of our tree"""
        return self.tree().root_path()

    def root_relative(self):
        """@return our root as relative path (string) to the root of our tree"""
        return self._root_relative

    def stable_since(self):
        """@return time in seconds since epoch at which the package contents change the last time we were watching it"""
        return self._changed_at
        
        
    ## -- End Interface -- @}
# end class Package

