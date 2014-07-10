#-*-coding:utf-8-*-
"""
@package tractool.db_list
@brief A command for querying tractor databases

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import os
import sys
import logging
from fnmatch import fnmatch

from . import base
from .utility import (
                        CacheParser,
                        CSVJob
                      )

import TrContext


class DBListSubCommand(base.TractorDBCommandBase, Plugin):
    """Implemnts listing the raw database in various ways"""
    __slots__ = (
                    'args',     # Our input argument namespace
                    'context'   # Basically a job database
                 )
    
    # -------------------------
    ## @name Baseclass Configuration
    # @{
    
    name = 'db-list'
    description = 'List and filter the raw command database. By default, we list all jobs in the database as csv'
    version = '0.1.0'
    
    ## -- End Baseclass Configuration -- @}
    
    # -------------------------
    ## @name Configuration
    # @{
        
    output_mode_job = 'job'
    output_mode_info = 'job-minimal'
    output_mode_task = 'task'
    output_modes = (output_mode_job, output_mode_info, output_mode_task)
    
    input_mode_all = 'all'
    input_mode_visible = 'visible'
    input_modes = (input_mode_all, input_mode_visible)
    
    ## -- End Configuration -- @}

    
    def setup_argparser(self, parser):
        super(DBListSubCommand, self).setup_argparser(parser)
        
        help = 'defines the kind of output we get.' + os.linesep
        help += "'%s' is suitable for command-line usage%s" % (self.output_mode_info, os.linesep) 
        help += "'%s' is the parsed job info with job details.%s" % (self.output_mode_job, os.linesep)
        help += "'%s' is all tasks of all encountered jobs including a path to their command log" %  self.output_mode_task
        help += "Default is '%s'" % self.output_mode_job
        parser.add_argument('-o', '--output-mode', dest='output_mode', default=self.output_mode_job, 
                            choices=self.output_modes, help=help)
        
        parser.add_argument('-u', '--user', dest='user', default=None,
                            help="If set, list only jobs of the given user")
        
        help = "Defines if we should iterate 'all' jobs, or just 'active' ones."
        help += "The less you iterate, the faster the operation is finished"
        parser.add_argument('-i', '--input-set', dest='input_mode', default=self.input_mode_all,
                             choices=self.input_modes, help=help)
        
        help = "If set, jobs or tasks will be filtered by their status."
        help += "Only works with output modes which are not '%s'" % self.output_mode_info
        parser.add_argument('--status', dest='status', default=None, choices=CSVJob.stati, help=help)
        
        help = "A filter based on a globbing pattern applied to the job ID" + os.linesep
        help += "Only works if not in '%s' mode" % self.output_mode_info
        parser.add_argument('--jid-glob', dest='jid_glob', default=None, help=help)
        
        help = "A globbing filter for the title of jobs. Only valid in modes which are not '%s'" % self.output_mode_info
        parser.add_argument('--job-title-glob', dest='job_title_glob', default=None, help=help)
        
        return self
        
    def _iter_jobs(self):
        """@return iterator over job information based on our current options"""
        if self.args.input_mode == self.input_mode_all:
            # SETUP ARGS
            find_args = dict()
            if self.args.user:
                find_args['user'] = self.args.user
            # end handle settings
            
            for jid, user, jobdir in self.context.FindJob(**find_args):
                if self.args.jid_glob and not fnmatch(str(jid), self.args.jid_glob):
                    continue
                    
                if self.args.output_mode == self.output_mode_info:
                    yield jid, user, jobdir
                else:
                    yield self.context.job(user, jid, jobdir=jobdir, mode=TrContext.TrModeObject)
                # end handle job conversion
        else:
            for jid, user in CacheParser(self.args.root).iter_entries():
                if ((self.args.user and self.args.user != user) or 
                    (self.args.jid_glob and not fnmatch(str(jid), self.args.jid_glob))):
                    continue
                job = self.context.job(user, jid)
                
                if self.args.output_mode == self.output_mode_info:
                    # This is expensive !
                    job_dir = job.locate_jobdir()
                    yield jid, user, job_dir
                else:
                    yield job
            #end for each cache entry
        #end handle input mode
        
    def execute(self, args, remaining_args):
        self.args = args
        
        # Verify Args Logic
        ####################
        if args.output_mode == self.output_mode_info:
            if args.status:
                print >> sys.stderr, "Cannot currently use status filtering if output mode is '%s'" % self.output_mode_info
                return self.ERROR
            # end handle job status logic
            if args.job_title_glob:
                print >> sys.stderr, "Cannot currently use job title filters if mode is '%s'" % self.output_mode_info
                return self.ERROR
        # end handle info mode
        
        self.context = context = TrContext.TrContext(mode=TrContext.TrModeInfo, rootdir=args.root, 
                                                    jobclass=CSVJob, logger=self.log())
        
        # HEADER
        #########
        if not args.skip_header:
            if args.output_mode == self.output_mode_job:
                sys.stdout.write(CSVJob.field_sep.join(CSVJob.job_columns) + os.linesep)
            elif args.output_mode == self.output_mode_info:
                sys.stdout.write(CSVJob.field_sep.join(('jid', 'userdir', 'root')) + os.linesep)
            elif args.output_mode == self.output_mode_task:
                sys.stdout.write(CSVJob.field_sep.join(CSVJob.task_columns) + os.linesep)
            else:
                assert False, "Couldn't handle output mode %s" % args.output_mode
            # end handle object mode
        # end if not skip header 
        
        jid = -1
        for jid, job in enumerate(self._iter_jobs()):
            if args.output_mode == self.output_mode_info:
                sys.stdout.write(CSVJob.field_sep.join(str(field) for field in job) + os.linesep)
            else:
                if args.job_title_glob and not fnmatch(job.title, args.job_title_glob):
                    continue
                # end filter by title
                if args.output_mode == self.output_mode_job:
                    if args.status and args.status != job.state_name():
                        continue
                    # status filtering
                    sys.stdout.write(job.to_csv() + os.linesep)
                elif args.output_mode == self.output_mode_task:
                    def may_print(task):
                        return args.status is None or args.status == job.state_name(task.state)
                    # end predicate
                    for task in job.tasklist.itervalues():
                        job.stream_task_as_csv(task, sys.stdout, predicate = may_print)
                    # end for each task
                else:
                    assert False, "Couldn't handle output mode %s" % args.output_mode
                # end handle output mode
        # end for each job
        
        if jid == -1:
            print >> sys.stderr, "Not a single job found at root '%s'" % args.root
            return self.ERROR
        # end handle invalid root
        return self.SUCCESS

# end class DBList


