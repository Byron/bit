#-*-coding:utf-8-*-
"""
@package dropbox.tests.test_base
@brief tests for dropbox.base

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import os
from time import time

from dropbox.base import *
from dropbox.tree import *
from dropbox.finder import *

from . import DropboxTestCase

from tx.core.kvstore import KeyValueStoreModifier
from bit.utility import set_default_encoding

# Assure we can handle filesystem objects somehow
set_default_encoding('utf-8')


class TestPackageDiffer(PackageDiffer):
    __slots__ = ('added_seen', 'removed_seen', 'changed_seen', 'unchanged_seen')

    def __init__(self):
        self.added_seen = self.removed_seen = self.changed_seen = self.unchanged_seen = 0

    def _handle_added_package(self, rhs_package):
        self.added_seen += 1

    def _handle_removed_package(self, lhs_package):
        self.removed_seen += 1
        
    def _handle_possibly_changed_package(self, lhs_package, rhs_package, modified):
        super(TestPackageDiffer, self)._handle_possibly_changed_package(lhs_package, rhs_package, modified)
        self.changed_seen += modified == True
        self.unchanged_seen += modified == False

# end class TestPackageDiffer


class BaseDropboxTestCase(DropboxTestCase):
    __slots__ = ()

    tree_a = 'tree/a'
    tree_b = 'tree/b'

    def test_finder(self):
        """Test for the dropbox finder implementation"""
        df = DropboxFinder(paths=[self.fixture_path('')], config_file_glob='dropbox.yaml')
        assert len(df.dropboxes) == 0
        assert len(list(df.iter_dropboxes())) == 0

        assert len(df.update(known_only=True).dropboxes) == 0, "there is nothing to update yet"
        assert len(df.update(known_only=False).dropboxes) == 0, "now it should have found nothing, search depth not high enough"

        df.max_depth = 2
        assert len(df.update().dropboxes) == 1, "search depth should now be sufficient"

        # Just update again to trigger some code - for now we trust it's capability to detect changes 
        # and obey it's arguments
        for known in range(2):
            assert len(df.update(known_only=known).dropboxes) == 1
        # end for each known item

        assert len(list(df.iter_dropboxes())) == 1, "should be same result"

        db = df.iter_dropboxes().next()
        for pkg in db.iter_packages():
            assert df.dropbox_by_contained_path(pkg.root()) is db
        # end for each package

    def test_dropbox(self):
        """Test basic dropbox type"""
        config = KeyValueStoreModifier(dict(package=dict(search_paths=[str(self.fixture_path(self.tree_a)),
                                                                       str(self.fixture_path(self.tree_b))])))
        db = Dropbox(config)
        assert len(db.trees()) == 2

        # can't clear cache without config path
        self.failUnlessRaises(AssertionError, db.clear_configuration_cache)
        assert db.clear_configuration_cache(configuration=config) is db

        # test package diff - there may be no change
        tpd = TestPackageDiffer()
        assert len(db.diff_tree_sample_packages(tpd)) == 9
        assert tpd.added_seen == tpd.removed_seen == tpd.changed_seen == 0
        assert tpd.unchanged_seen == 9

        dp = Dropbox(self.fixture_path('tree/dropbox.yaml'))
        assert len(dp.trees()) == 2, "initialized from a file, the result should be the same"

    def test_tree(self):
        """Test the tree type's basic functionality"""
        tree = TreeRoot(self.fixture_path(self.tree_a))
        assert tree.root_path() is not None
        assert tree.sample_time() is not None
        assert len(tree) == 4

        assert not isinstance(tree['file.ext'], dict), "should obtain file"
        assert isinstance(tree['dir_full'], dict), "should have gotten dict"
        assert not isinstance(tree['dir_full/file.ext'], dict), "should have gotten file"
        assert not isinstance(tree['first_level_empty/subdir/package_dir/empty.file'], dict), 'should have gotten file'

        packs = sorted(tree.iter_packages())
        assert len(packs) == 5, "Should have exactly 4 packages"

        rela_packs = ['7.3.\xc2\xa0package-cleanup.html', 'dir_full', 'file.ext', 'first_level_empty/package_dir', 'first_level_empty/subdir/package_dir']
        assert [p.root_relative() for p in packs] == rela_packs, "didn't get expected packages"

        # Test testing for changes and affected packages
        tree2 = TreeRoot(self.fixture_path('tree/a'))
        assert tree2 == tree, "tree comparison should work"
        
        packs2 = sorted(tree2.iter_packages())
        pid = 2
        assert packs2[pid].set_stable_since(time()) is packs2[pid]
        assert packs2[pid] == packs[pid], "Package comparison should work"

        # change contents of file, and comparison should change
        assert packs2[pid].root().isfile()
        tree2[packs2[pid].root_relative()] = os.stat('.')
        assert packs2[pid] != packs[pid]
        assert packs[1].root().isdir() and packs[0] != packs[pid], "Should be able to compare file and directory packages"
        assert len(packs[1].entries())

        assert packs[pid].set_tree(tree2) is packs[pid], "tree can be changed to anything compatible"
        # set it back ... for diffing later
        assert packs[pid].set_tree(tree) is packs[pid], "tree can be changed to anything compatible"

        # prepare tree for diff
        #######################
        # Add fake package
        packs2.append(Package(tree2, 'first_level_empty'))
        # remove package
        del packs2[0]
        tpd = TestPackageDiffer()
        rhspacks = tpd.diff(packs, packs2)

        assert tpd.added_seen == tpd.removed_seen == tpd.changed_seen == 1
        assert tpd.unchanged_seen == 3

        assert len(packs) == 5
        tpd = TestPackageDiffer()
        assert tpd.diff(list(), packs)
        assert tpd.added_seen == 5

        # Entries
        c = 0
        for name in tree:
            c += 1
            assert len(tree.entries(name))
        # end for each name
        assert c
