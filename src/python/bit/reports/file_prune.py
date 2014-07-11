#-*-coding:utf-8-*-
"""
@package bit.reports.file_prune
@brief A module with a report to prune files which exist in duplicates

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import os
import sys
from fnmatch import fnmatch


from .base import ReportGenerator
from .version import VersionReportGenerator

import bapp
from bcmd import InputError
from butility import (Path,
                      int_to_size_string)

from bit.utility import (  delta_to_tty_string,
                           seconds_to_datetime,
                           datetime_to_seconds,
                           ravg,
                           rsum,
                           none_support,
                           DistinctStringReducer)

from butility import Path
from stat import S_ISLNK



class FilePruneReportGenerator(ReportGenerator, bapp.plugin_type()):
    """Generates a report stating the duplication state of a certain directory tree compared to any amount of source trees, 
    based on file-names.

    Files with a similar name are assumed to be similar !
    """
    __slots__ = ()

    type_name = 'file-prune'
    description = "Generates a report stating the duplication state of a certain directory tree compared to any amount\
                   of source trees, based on file-names."

    report_schema = (   ('filename', str, str, DistinctStringReducer()),
                        ('filepath', str, str, DistinctStringReducer()),
                        ('size', str, none_support(int_to_size_string), rsum),
                        ('mode', int, none_support(lambda m: '%o' % m)),
                        ('reason', str, str),
                    )

    _schema = ReportGenerator._make_schema(type_name, dict(file_glob="*.rpm", # glob by which to find files of interest
                                                               script=dict(remove_symlink_destination = True,
                                                                           file_remove_command = "rm -vf"
                                                                            )
                                                          ))

    # -------------------------
    ## @name Utilities
    # @{

    def _recurse_directory(self, dir, glob, closure):
        """Recurse into 'dir' and call closure(dir, file) with each file matching given glob pattern"""
        for root, dirs, files in os.walk(dir):
            for file in files:
                if fnmatch(file, glob):
                    closure(root, file)
                # end file matches
            # end for each file
        # end for each iteration
        
    def _build_report(self, config, args):
        """@return TBD"""
        report = self.ReportType(columns=self.report_schema)
        record = report.records.append

        repo = dict()
        prune_candidates = list()
        def gather_info(dirs, type_name, closure):
            for dir in dirs:
                if not dir.isdir():
                    raise InputError("%s directory at '%s' wasn't accessible" % (type_name, dir))
                # end assert input
                sys.stderr.write("Gathering '%s' %s file-info in '%s'\n" % (config.file_glob, type_name, dir))
                self._recurse_directory(dir, config.file_glob, closure)
            # end for each source
        # end helper

        gather_info(args.source_trees, 'source', lambda dir, file: repo.__setitem__(file, Path(dir) / file))
        gather_info(args.prune_trees, 'prune', lambda dir, file: prune_candidates.append(Path(dir) / file))

        sys.stderr.write("Finding duplicates in set of %i files\n" % len(prune_candidates))
        for path in prune_candidates:
            bn = path.basename()
            if bn in repo:
                record((bn, 
                        path,
                        path.stat().st_size,
                        path.lstat().st_mode,
                        "exists at '%s'" % repo[bn]))
            # end setup prune record
        # end for each path to possibly prune 

        record(report.aggregate_record())
        return report

    def _sanitize_configuration(self, config, args):
        """@return configuration which is assured to have the correct type of values, or None, None on error"""
        if not args.source_trees:
            raise InputError("Please specify at least one source tree")
        # end
        if not args.prune_trees:
            raise InputError("Please specify at least one tree to be pruned")

        if not config.file_glob:
            raise InputError("A file glob must be given")

        return config, args

    ## -- End Utilities -- @}

    # -------------------------
    ## @name Subclass Implementation
    # @{

    @classmethod
    def _setup_argparser(cls, parser):

        help = "A directory whose files, parsed recursively, add to the source list of available files against which the "
        help += "directory to be pruned should be compared to. At least one must be set"
        parser.add_argument('-s', '--source-trees', dest='source_trees', nargs='+', type=Path, help=help)

        help = "The directory checked for files which possibly exist in one of the source tree."
        help += "These will be pruned as needed."
        parser.add_argument('-p', '--prune-trees', dest='prune_trees', nargs='+', type=Path, help=help)

        return super(FilePruneReportGenerator, cls)._setup_argparser(parser)
    
    ## -- End Subclass Implementation -- @}

    # -------------------------
    ## @name Interface Implementation
    # @{

    def generate(self):
        report = self.ReportType(columns=self.report_schema)
        record = report.records.append
        config, args = self._sanitize_configuration(self.configuration(), self.arguments())
        return self._build_report(config, args)

    def generate_fix_script(self, report, writer):
        """Generate a script which removes files individually as well as empty folders"""
        # NOTE: this is copy past from version.py
        config = self.configuration()
        VersionReportGenerator.delete_script_safety_prefix(writer)

        # Sort by directory
        dir_map = dict()
        for fn, path, size, mode, reason in report.records:
            if not isinstance(path, Path):
                continue
            # end safely skip aggregate

            dir = path.dirname()
            dir_map.setdefault(dir, list()).append(path.basename())

            # This works because our script enters the directory dir before doing the deletion
            # Our link following is non-recursive, which should be fine
            if config.script.remove_symlink_destination and S_ISLNK(mode):
                dir_map[dir].append(path.readlink())
            # end handle symlink removal
        # end for each entry
        
        fcount = 0
        for dir, files in dir_map.iteritems():
            writer('if cd "%s"; then\n' % dir)
            writer('    xargs -n 100 $prefix %s <<FILES\n' % config.script.file_remove_command)
            for file in files:
                fcount += 1
                writer(file + '\n')
            # end for each file
            writer('FILES\n')
            writer('fi\n')
        # end for each dir

        dcount = 0
        writer('xargs -n 50 $prefix rmdir --ignore-fail-on-non-empty <<DIRS 2>/dev/null\n')
        for dir in sorted(dir_map.keys(), key=lambda d: len(d), reverse=True):
            dcount += 1
            writer(dir + '\n')
        # end for each directory
        writer('DIRS\n')
        writer('echo "Removed up to %i files and %i directories"\n' % (fcount, dcount))
        return True
    
    ## -- End Interface Implementation -- @}

# end class FilePruneReportGenerator

