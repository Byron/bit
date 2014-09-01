#-*-coding:utf-8-*-
"""
@package bit.cmd.daemon
@brief implements a daemon in the standard command framework, with most basic functionality.

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DaemonCommand']

import sys
import os
import signal
from time import sleep

from bcmd import Command
from butility import Path
from .base import OverridableSubCommandMixin
                    

class DaemonCommand(OverridableSubCommandMixin, Command):
    """Main Daemon command without subcommands. Just starts a thread which can be an ApplicationSettingsMixin
    """
    __slots__ = ()

    # -------------------------
    ## @name Configuration
    # @note all Command configuration must be provided too
    # @{

    ## The kind of TerminatableThread, which must be a context client, to daemonize
    # Must be set in subclass
    ThreadType = None
    
    ## -- End Configuration -- @}


    # -------------------------
    ## @name Utilities
    # @{
    @classmethod
    def daemonize(cls, pid_file):
        """Daemonize ourselves to become independent of the caller
        @param pid_file path to file to prevent multiple daemons to run at once. Will always write it with our pid
        """
        
        if sys.platform.startswith("win"):
            raise OSError("Cannot daemonize on windows")
        # END handle operating system
        
        try:
            # Fork a child process so the parent can exit.  This returns control to
            # the command-line or shell.    It also guarantees that the child will not
            # be a process group leader, since the child receives a new process ID
            # and inherits the parent's process group ID.  This step is required
            # to insure that the next call to os.setsid is successful.
            pid = os.fork()
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)
    
        if (pid != 0):
            # exit() or _exit()?
            # _exit is like exit(), but it doesn't call any functions registered
            # with atexit (and on_exit) or any registered signal handlers.   It also
            # closes any open file descriptors.  Using exit() may cause all stdio
            # streams to be flushed twice and any temporary files may be unexpectedly
            # removed.  It's therefore recommended that child branches of a fork()
            # and the parent branch(es) of a daemon use _exit().
            os._exit(0)
        # END exit 
            
        ##################
        # The first child.
        ##################
        # To become the session leader of this new session and the process group
        # leader of the new process group, we call os.setsid(). The process is
        # also guaranteed not to have a controlling terminal.
        os.setsid()

        # Is ignoring SIGHUP necessary?
        #
        # It's often suggested that the SIGHUP signal should be ignored before
        # the second fork to avoid premature termination of the process.    The
        # reason is that when the first child terminates, all processes, e.g.
        # the second child, in the orphaned group will be sent a SIGHUP.
        #
        # "However, as part of the session management system, there are exactly
        # two cases where SIGHUP is sent on the death of a process:
        #
        #    1) When the process that dies is the session leader of a session that
        #        is attached to a terminal device, SIGHUP is sent to all processes
        #        in the foreground process group of that terminal device.
        #    2) When the death of a process causes a process group to become
        #        orphaned, and one or more processes in the orphaned group are
        #        stopped, then SIGHUP and SIGCONT are sent to all members of the
        #        orphaned group." [2]
        #
        # The first case can be ignored since the child is guaranteed not to have
        # a controlling terminal.   The second case isn't so easy to dismiss.
        # The process group is orphaned when the first child terminates and
        # POSIX.1 requires that every STOPPED process in an orphaned process
        # group be sent a SIGHUP signal followed by a SIGCONT signal.   Since the
        # second child is not STOPPED though, we can safely forego ignoring the
        # SIGHUP signal.    In any case, there are no ill-effects if it is ignored.
        #
        # import signal           # Set handlers for asynchronous events.
        # signal.signal(signal.SIGHUP, signal.SIG_IGN)

        try:
            # Fork a second child and exit immediately to prevent zombies.   This
            # causes the second child process to be orphaned, making the init
            # process responsible for its cleanup.   And, since the first child is
            # a session leader without a controlling terminal, it's possible for
            # it to acquire one by opening a terminal in the future (System V-
            # based systems).    This second fork guarantees that the child is no
            # longer a session leader, preventing the daemon from ever acquiring
            # a controlling terminal.
            pid = os.fork() # Fork a second child.
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)

        if (pid != 0):
            # exit() or _exit()?     See below.
            os._exit(0) # Exit parent (the first child) of the second child.
        # END exit second child

        # Decouple stdin, stdout, stderr
        fd = os.open(os.devnull, os.O_RDWR) # standard input (0)
        
        # Finally, write our PID file
        open(pid_file, 'wb').write(str(os.getpid()))
    
        # Duplicate standard input to standard output and standard error.
        os.dup2(fd, 1)           # standard output (1)
        os.dup2(fd, 2)           # standard error (2)
        

    def _sighandler_term(self, signum, frame, dt):
        """Handle termination of the main thread"""
        self.log().info("Process interrupted - please wait while threads are being stopped ...")
        dt.stop_and_join()

    ## -- End Utilities -- @}

    def setup_argparser(self, parser):
        super(DaemonCommand, self).setup_argparser(parser)

        assert self.ThreadType is not None, "ThreadType must be set in subclass"

        help = "Start ourselves as daemon and write the PID to the given path."
        help += "Fails if the file already exists - we won't check for orphaned files"
        parser.add_argument('--pid-file', '-d', dest='pid_file', type=Path, help=help)

        help = "Show the daemons effective configuration and exit"
        parser.add_argument('--show-configuration', '-c', default=False, 
                                dest='show_config', action='store_true', help=help)

        return self

    def execute(self, args, remaining_args):
        self.apply_overrides(self.ThreadType.settings_schema(), args.overrides)

        if args.show_config:
            print "%s.*" % self.ThreadType.schema().key()
            print self.ThreadType.settings_value()
            return self.SUCCESS
        # end handle config printing

        # Whatever happens, make sure we delete the pid file
        if args.pid_file is not None:
            if args.pid_file.isfile():
                self.log().error("PID file at '%s' exists - daemon is already running. Otherwise, delete the file and retry", args.pid_file)
                args.pid_file = None
                return self.ERROR
            # end handle pid file
            self.daemonize(args.pid_file)
        # end handle daemonization

        try:
            dt = self.ThreadType()
            dt.start()

            signal.signal(signal.SIGTERM, lambda sig, frame: self._sighandler_term(sig, frame, dt))

            self.log().info("Running in debug mode - press Ctrl+C to interrupt")
            try:
                # Wait for it to come up
                sleep(0.1)
                # Thread will run forever, we have to watch for interrupts
                while dt.is_alive():
                    sleep(0.1)
                # end wait loop
            except (KeyboardInterrupt, SystemExit):
                self._sighandler_term(15, None, dt)
            except Exception:
                self.log().error("Unknown exception occurred", exc_info=True)
                return self.ERROR
            else:
                # it terminated ... can have it's reason
                self.log().info("Daemon thread terminated - please see log for details")
                return self.SUCCESS
            # end handle interrupts

            return self.SUCCESS
        finally:
            if args.pid_file and args.pid_file.isfile():
                args.pid_file.remove()
            # end remove pid file
        #end handle pid file


# end class DaemonCommand

