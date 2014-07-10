#-*-coding:utf-8-*-
"""
@package tractool.utility
@brief shared types for general use

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['CacheParser', 'CSVJob']

import os

import TrJob


class CSVJob(TrJob.TrJob):
    """Utility to help dealing with Job Objects
    @todo generalize this to a simple Matrix with columns"""
    __slots__ = ()
    
    field_sep = ';'
    job_columns = ('jid', 'user', 'host', 'title', 'priority', 'state', 'statetime', 'spooltime', 'starttime', 
                   'donetime', 'slottime', 'deletetime', 'ntasks', 'active', 'blocked', 'done', 'ready', 'error',
                   'comment')
    
    task_cmd_log_path = 'cmd-log-path'
    task_columns = ('jid', 'tid', 'sjid', 'title', 'state', 'statetime', 'readytime', task_cmd_log_path)
    
    status_active = 'active'
    status_done = 'done'
    status_error = 'error'
    status_blocked = 'blocked'
    status_unset = 'unset'
    stati = (status_done, status_error, status_active, status_blocked, status_unset)
    
    def to_row(self):
        """@return tuple of resolved field values from our own instance """
        return tuple(getattr(self, attr) for attr in self.job_columns)
        
    def to_csv(self):
        """@return string of ourselves as CSV"""
        return self.field_sep.join(str(field) for field in self.to_row())
        
    def _task_log_path(self, task):
        """@retrun log directory for given task
        @todo download the tractor configuration file and obtain the info from there ... ! Its hardcoded right now"""
        return '%s/cmd-logs/%s/J%s/T%s.log' % (self.rootdir, self.user, self.jid, task.tid)  
        
    def stream_task_as_csv(self, task, stream, predicate = lambda task: True):
        """Write a csv representation recursively of all our tasks to the given stream. 
        Each task has a log entry if the file exists on disk.
        @param predicate function(task) -> Bool to indicate if the given task should be printed
        @note iteration is breadth first"""
        # print info to stream (then recurse) == bread
        assert self.task_columns[-1] == self.task_cmd_log_path
        if predicate(task):
            stream.write(self.field_sep.join(str(getattr(task, field)) for field in self.task_columns if field !=self. task_cmd_log_path))
            stream.write(self.field_sep + self._task_log_path(task) + os.linesep)
        # end if task should be printed
        for child_task in task.tasklist.itervalues():
            self.stream_task_as_csv(child_task, stream, predicate)
        # end for each child_task
        
    def locate_jobdir(self):
        """@return path at which this can be found
        @note much faster than locateJobDir. Will cache result as well"""
        if self.jobdir:
            return self.jobdir
        
        self.jobdir = TrJob.jobdirPath(self.user, self.jid, self.rootdir)
        return self.jobdir
        
    def state_name(self, state=None):
        """@return the long name of our status
        @param state if None, the job state will be used. Otherwise, the given one"""
        state = state or self.state
        ss = state.lower()
        for name in self.stati:
            if name[0] == ss:
                return name
        # end for each name to check
        assert False, "didn't find status name for internal status '%s'" % state
        
    
# end class CSVJob


class CacheParser(object):
    """Simple utility to parse the tractor cache"""
    __slots__ = ('root')
    
    def __init__(self, root):
        """Intiialize ourselves with the root path
        @param root Path instance to tractor db root"""
        self.root = root
        
    # -------------------------
    ## @name Interface
    # @{
    
    def iter_entries(self, predicate=None):
        """@return generator yielding tuples of jobid (int) and owner name
        @param predicate function(jid, user) => Bool function returning True for each jid, user tuple that 
        should be output in the iteration"""
        visible_cache = self.cache_file()
        if not visible_cache.isfile():
            raise EnvironmentError("Visible jobs cache file was expected at '%s' but didn't exist" % visible_cache)
        # end assure file exists
        
        for line in visible_cache.lines(retain=False):
            if line.startswith('#'):
                self._handle_comment(line)
                continue
            fields = line.split(',')
            assert len(fields) == 2, "expecting 'jid, user'"
            rval = (int(fields[0]), fields[1].strip())
            
            if predicate and not predicate(*rval):
                continue
            #end handle predicate
            
            yield rval
        #end for each line
        
    def cache_file(self):
        """@return expected default location of the cache file based on our root"""
        return self.root / 'caches' / 'visiblejobs.txt'
        
    ## -- End Interface -- @}
    
    # -------------------------
    ## @name Subclass Interface
    # @{
    
    def _handle_comment(self, line):
        """Called whenever a comment is encountered during iter_entries()
        @param line comment without line ending"""
        
    ## -- End Subclass Interface -- @}
        
# end class CacheParser

