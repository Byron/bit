#-*-coding:utf-8-*-
"""
@package bit.reports.base
@brief Base types for generating reports

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ReportGeneratorBase', 'Report']

import tx
from bit.utility import Table
from tx.core.component import EnvironmentStackContextClient
from tx.core.kvstore import KeyValueStoreSchema


class Report(Table):
    """A report is essentially a table, with the ability to serialize itself to common formats.
    @note reports keep their values plain, but assume their conversion by the schema's constructor before 
    serialization."""
    __slots__ = ()

    # -------------------------
    ## @name Configuration
    # @{

    ## Constant indicating you want comma separated values serialization
    SERIALIZE_CSV = 'csv'

    ## Serialize to look pretty in a tty
    SERIALIZE_TTY = 'tty'
    
    ## -- End Configuration -- @}

    # -------------------------
    ## @name Interface
    # @{

    def serialize(self, mode, writer, column_names=True):
        """Serialize this instance in the given mode to the given writer, which will be handed the text to write.
        @param column_names if True, columns names will be printed as first line
        @return this instance"""
        if self.is_empty():
            return self
        # end early bailout

        cols = self.columns
        colnames = [t[0] for t in cols]
        recs = self.records
        sep = ';'

        if mode == self.SERIALIZE_CSV:
            if column_names:
                writer(sep.join(colnames) + '\n')
            # end handle column names
            for rec in recs:
                converted = list()
                for vid, val in enumerate(rec):
                    if vid > 0:
                        writer(sep)
                    writer(str(val))
                # end for each columns value
                writer('\n')
            # end for each record
        elif mode == self.SERIALIZE_TTY:
            # TODO: pre-compute required column sizes and print accordingly
            widths = [0] * len(colnames)
            for cid, name in enumerate(colnames):
                widths[cid] = len(name)
            # end for each column
            for rec in recs:
                for vid, val in enumerate(rec):
                    widths[vid] = max(widths[vid], len(str(cols[vid][2](val))))
                # end for each value
            # end for each record

            tab = '  '
            space = ' '
            last_column = len(colnames) - 1

            def write_col(cid, string):
                """Write given string into a column of the given column ID at correct size"""
                if cid > 0:
                    writer(tab)
                writer(string)
                if cid == last_column:
                    writer('\n')
                else:
                    # last column doesn't need to be filled
                    writer((widths[cid] - len(string)) * space)
            # end write column

            if column_names:
                for cid, name in enumerate(colnames):
                    write_col(cid, name.upper())
                # end for each column
            # end handle column names

            for rec in recs:
                for vid, val in enumerate(rec):
                    write_col(vid, str(cols[vid][2](val)))
                # end for each value
            # end for each record
        else:
            raise AssertionError('Mode not implemented')
        # end handle mode

        return self
    ## -- End Interface -- @}
# end class Report


class ReportGeneratorBase(EnvironmentStackContextClient):
    """Interface for report generators"""
    __slots__ = ('_args')

    # -------------------------
    ## @name Configuration
    # @{

    ## KeyValueStoreSchema at the correct key to define configuration data for a report of our type
    # Use ReportGeneratorBase._make_schema(...) to aid the process
    _schema = None
    
    ## Type of report our subtype should instantiate during generate
    ReportType = Report

    ## Defines a type-name for this class
    # Must be set by subclass
    type_name = None

    ## A description saying what your report does.
    # Should be set in subclass
    description = None


    ## -- End Configuration -- @}


    # -------------------------
    ## @name Constants
    # @{

    ## Keyname for kvstore at which to store configuration values for all reports
    REPORT_ROOT_KEY = 'itool.report'
    
    ## -- End Constants -- @}

    def __init__(self, arguments):
        """Initialize this instance with parsed arguments
        @param arguments compatible to Argparse"""
        self._args = arguments
    
    # -------------------------
    ## @name Subclass Utilities
    # @{

    @classmethod
    def _make_schema(cls, type_name, schema_dict):
        """@return a KeyValueStoreSchema instance using the correct key with the given schema_dict
        @param type_name our types type_name
        @param schema_dict a possibly nested dictionary"""
        return KeyValueStoreSchema('%s.%s' % (cls.REPORT_ROOT_KEY, type_name), schema_dict)

    @classmethod
    def _setup_argparser(cls, parser):
        """Given the parser of the main command, you may configure a subparser to your liking.
        The base-implementation must be called in any way though, as it may add some defaults"""
        return
        
        

    ## -- End Subclass Utilities -- @}

    # -------------------------
    ## @name Interface
    # @{

    def arguments(self):
        """@return commandline arguments parsed for use. See _setup_argparser() """
        return self._args

    def configuration(self):
        """@return a dictionary with all our custom configured values"""
        return self.context_value()

    def error(self):
        """@return a True value if we suffered an error while generating the report, or False otherwise.
        Reports with errors should not be trusted, they might be incomplete, and the intermediate result was 
        just returned for you to have something.
        Your generator can output additional information through stderr.
        @note default False"""
        return False

    @tx.abstractmethod
    def generate(self):
        """Produce the report based on this instance's configuration
        @return a Report instance, which must be an instance of self.ReportType"""
        
    @tx.abstractmethod
    def generate_fix_script(self, report, writer):
        """Based on the given report (as previously generated by this instance, produce a shell script to stream
            which would be able to fix the issue if executed on the right host.
        @return True if a script was generated, False if this is not implemented.
        """
        
    ## -- End Interface -- @}

    

# end class ReportGeneratorBase

