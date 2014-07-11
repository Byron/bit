#-*-coding:utf-8-*-
"""
@package bit.cmd.report
@brief A command for generating any kind of report

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ReportCommandMixin']

import sys

import bapp
from .base import OverridableSubCommandMixin
from bit.reports import Report



class ReportCommandMixin(OverridableSubCommandMixin, bapp.plugin_type()):
    """Use reports (as plugins) whose interface is made available through the commandline"""
    __slots__ = ()

    name = 'report'
    description = 'Generate reports based on report plugins'
    version = '0.1.0'

    # -------------------------
    ## @name Constants
    # @{

    OUTPUT_SCHEMA = 'query-config'
    OUTPUT_GENERATE_TTY = 'generate'
    OUTPUT_GENERATE_CSV = 'generate-csv'
    OUTPUT_GENERATE_SCRIPT = 'generate-script'
    output_schemas = (OUTPUT_SCHEMA, OUTPUT_GENERATE_TTY, OUTPUT_GENERATE_CSV, OUTPUT_GENERATE_SCRIPT)

    ## -- End Constants -- @}

    # -------------------------
    ## @name Configuration
    # @{

    ## A base class for all report plugins we can handle
    # Note: must be set in subclass
    ReportBaseType = None
    
    ## -- End Configuration -- @}

    # -------------------------
    ## @name Utilities
    # @{

    @classmethod
    def report_types(cls):
        """@return all report types currently registered"""
        assert cls.ReportBaseType is not None, "ReportBaseType must be set in subclass"
        return self.application().context().types(cls.ReportBaseType)

    ## -- End Utilities -- @}

    def setup_argparser(self, parser):
        super(ReportCommandMixin, self).setup_argparser(parser)

        types = self.report_types()
        assert types, "Didn't find a single report"

        help = 'The name of the report to run'
        spg = parser.add_subparsers(title="Reports", help=help)
        for cls in types:
            assert cls.type_name
            desc = cls.description or "No description provided"
            subparser = spg.add_parser(cls.type_name, description = desc, help = desc)
            cls._setup_argparser(subparser)

            help = 'The kind of output you want.'
            help += "%s: show all configured values influencing the report." % self.OUTPUT_SCHEMA
            subparser.add_argument('mode', choices=self.output_schemas, help=help)
            subparser.set_defaults(report_type=cls)
        # end for each type

        # parser.add_argument('--', dest='terminator', action='store_true',
        #                     help='Use this flag to indicate the parsing of variable length -s or -l report flags is done')
        return self

    def execute(self, args, remaining_args):
        generator = args.report_type(args)
        self.apply_overrides(generator.schema(), args.overrides)

        if args.mode == self.OUTPUT_SCHEMA:
            print >> sys.stderr, (generator.configuration())
        elif args.mode in (self.OUTPUT_GENERATE_CSV, self.OUTPUT_GENERATE_TTY):
            mode = (args.mode == self.OUTPUT_GENERATE_CSV) and Report.SERIALIZE_CSV or Report.SERIALIZE_TTY
            report = generator.generate()
            if report.is_empty():
                print >> sys.stderr, "Report didn't yield a result"
            else:
                report.serialize(mode, sys.stdout.write)
            # end handle no results
        elif args.mode == self.OUTPUT_GENERATE_SCRIPT:
            # make a report, then write a fix script
            generator.generate_fix_script(generator.generate(), sys.stdout.write)
            print ""
        else:
            raise NotImplementedError("'%s' mode not implemented" % args.mode)
        # end handle mode

        return generator.error() and self.ERROR or self.SUCCESS

# end class ReportCommandMixin
