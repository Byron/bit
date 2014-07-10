#-*-coding:utf-8-*-
"""
@package fsmonitor.daemon.base
@brief Basic daemon and scheduler implementation

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['DaemonThread']

import threading
import logging
import sys

from Queue import (Queue,
                   Empty)
from time import (time, 
                  sleep)

import bapp
from ..base import Dropbox
from .utility import (SQLPackageDifferMixin,
                      SessionWorkerThread,
                      ResultLoggerThread,
                      DaemonDropboxFinderMixin)

from bit.utility import ( TerminatableThread,
                            FrequencyStringAsSeconds )
from bkvstore import (KeyValueStoreSchema,
                             PathList,
                             AnyKey)
from bapp import ApplicationSettingsMixin
from fsmonitor.sql import (PackageSession,
                         SQLPackageTransaction,
                         with_threadlocal_session)

from fsmonitor.transaction import DropboxTransactionBase

log = logging.getLogger('dropbox.daemon')


class DaemonThread(TerminatableThread, ApplicationSettingsMixin, DaemonDropboxFinderMixin, SQLPackageDifferMixin):
    """A never-ending, auto-updating process which supervises dropboxes and tasks related to it.

    It is meant to run as thread, and uses the KV store for configuration.
    It will monitor search paths and automatically find new dropboxes as they are created, and remove them as they
    are deleted.

    Dropboxes will be sampled, and we will handle added or removed TreeRoots accordingly.
    All work is done via queues and workers, timers define how often we do certain kinds of work, and what should 
    be done if IO isn't fast enough.

    The daemon uses an SQL schema for persistence.

    We use the standard logging facility to inform about what happened regularly. The thread should be stopped
    using the standard means of a TerminatableThread.

    Caveats
    =======

     + Daemon needs to be restarted if configuration was changed.
     + If packages are removed while the daemon is not running, it will not notice this and therefore not update
       the database.
     + Recognizes new folders in packages as changes even if they are empty.

    """
    # Slots are disabled, as multiple bases with slots don't work ... crappy python, I hate it 
    _slots_ = (  '_update_dropboxes_scheduled',
                 '_ops_queue',
                 '_update_queue',
                 '_config',
                 '_url')

    ## Amount of workers we can have (per worker type)
    MAX_WORKERS = 17

    ## Amount of percent full the update queue must still be to prevent an update pass to be scheduled
    # as measured compared to the last seen count
    MAX_UPDATE_QUEUE_SCHEDULE_TASKS = 40

    # -------------------------
    ## @name Configuration
    # @{

    _schema = KeyValueStoreSchema('dropbox.daemon', {'search' : {'paths' : PathList,                # paths at which to search dropboxes
                                                                 'max_directory_depth' : 1,         # recursive depth for config file search
                                                                 'config_file_glob' : '.dropbox.yaml'},    # glob to use to find a dropbox configuration
                                                         'check' : { 'dropboxes_every' : FrequencyStringAsSeconds('60s'),          # how often to check for new dropboxes
                                                                     'packages_every' : FrequencyStringAsSeconds('15s'),           # how often to check for packages in each dropbox
                                                                     'transactions_every' : FrequencyStringAsSeconds('15s')        # how often to check for finished transactions that need handling
                                                                 },
                                                         'default_encoding' : 'utf-8',  # Encoding to use within python
                                                         'threads' : {'num_update_threads' : 1,         # how many parallel updates (dropboxes, packages)
                                                                  'num_operation_threads' : 2},     # how many parallel operations (copy, delete, check)
                                                         'db' : {'url' : str},                      # SQLAlchemy url
                                                         'authentication' : {'privileged_group' : 'role-data-io'},  # group required to be part of to authenticate
                                                         Dropbox.TRANSACTIONS_KEY : {AnyKey : dict()}, # optionally, we can contain base values for transaction configuration as well.
                                                        })


    ## -- End Configuration -- @}

    def __init__(self):
        """Init our variables"""
        super(DaemonThread, self).__init__()
        self.daemon = True
        self._update_dropboxes_scheduled = False
        
        self._config = config = self.settings_value()
        assert config.search.paths, "Need to specify at least one dropbox search path"
        self._ops_queue    = Queue()
        self._update_queue = Queue()

        # init mixins
        SQLPackageDifferMixin.__init__(self)
        DaemonDropboxFinderMixin.__init__(self, 
                                            config.search.paths, 
                                            config.search.max_directory_depth, 
                                            config.search.config_file_glob)

        

    # -------------------------
    ## @name Thread Tasks
    # @{

    def _update_dropboxes(self):
        """Just call's update on finder, but keeps track on active operations of this kind"""
        try:
            log.debug("Performing dropbox update ...")
            last_count = len(self.dropboxes)
            self.update()
            count_diff = len(self.dropboxes) - last_count

            if count_diff > 0:
                log.info("Added %i dropboxes", count_diff)
            elif count_diff < 0:
                log.info("Lost track of %i dropboxes", count_diff)
            else:
                log.debug("Dropbox count unchanged at %i", len(self.dropboxes))
            # end handle logging
        finally:
            self._update_dropboxes_scheduled = False
        # end handle singleton mechanism

    def _handle_packages_diff(self, db_key):
        """Called to diff packages, and schedule handling of whatever has to be done.
        It's really as simple as using our package differ implementation to do all the work
        @param db_key key into our finder to be sure we don't try to handle dropboxes which have been deleted 
        in the meanwhile
        """
        try:
            db = self[db_key]
        except KeyError:
            log.debug("Ignoring packages of Dropbox which was deleted in the meanwhile")
            return
        # end handle db deleted

        # Check if we can actually handle the dropbox
        db_updates_seconds = db.settings().update_packages_every.seconds
        if db_updates_seconds == 0 or \
           not db.last_tree_sample_time() or \
           time() >= db.last_tree_sample_time() + db_updates_seconds:

            log.debug("Handling packages of %s", db)
            db.diff_tree_sample_packages(self)
        else:
            log.debug("Skipped packages of %s as it's just updated every %s (waiting additional %.02fs)", db, db_updates_seconds, db.last_tree_sample_time() + db_updates_seconds - time())
        # end handle per-dropbox timings


    @with_threadlocal_session
    def _check_transactions(self, session):
        """Query all transactions which don't have an 'needs approval' marker, but are not yet queued, 
        and queue them if possible."""
        PT = SQLPackageTransaction
        for sql_trans in session.transactions().filter((PT.finished_at == None) & 
                                                       (PT.percent_done == None) & 
                                                       (PT.approved_by_login != None) & 
                                                       (PT.approved_by_login != PT.TO_BE_APPROVED_MARKER)):
            assert sql_trans.in_package, "For some reason, there is a transaction without in-package, check %s" % sql_trans
            sql_package = sql_trans.in_package

            try:
                db = self.dropbox_by_contained_path(sql_package.root())
            except ValueError:
                log.error("Couldn't find dropbox managing path at '%s' - dropbox might have been removed without removing transactions, which shouldn't happen ! Will cancel related transaction %s", sql_package.root(), sql_trans)
                # fix this
                sql_trans.cancel().commit()
                continue
            # end play it really save

            # It could be possible that we get here without the dropbox having had the chance to parse its 
            # trees yet.
            # This happens if we just restarted, but if there are still transactions in the DB
            # We don't want to trigger an update of the trees here, which is why we skip this entirely
            if db.last_tree_sample_time() is None:
                log.info("Didn't have parsed packages trees for dropbox %s yet - waiting until next transaction check", db)
                continue
            # end skip no cached trees

            # find package matching the sql package
            package = None
            for pkg in db.iter_packages():
                if ((pkg.tree_root()     == sql_package.tree_root()) and 
                    (pkg.root_relative() == sql_package.root_relative())):
                   package = pkg
                   break
                # end check for matching tree_root and package relative path
            # end for each package

            # This also indicates inconsistency - make sure we don't see the transaction again !
            if package is None:
                log.error("Failed to find matching package for sql package %s. Will cancel related transaction %s", sql_package, sql_trans)
                sql_trans.cancel().commit()
                continue
            # end handle inconsistency

            # if we are here, there is no queued transaction, and we want to recheck for approval
            auth_token = sql_trans.authentication_token(user_group=self._config.authentication.privileged_group)
            if auth_token in (PT.AUTH_OK, PT.AUTH_NOT_NEEDED):
                # NOTE: if it wouldn't need approval, we wouldn't even be here as it would be running or 
                # finished
                # ... and queue the transaction
                trans_cls = self._transaction_cls_by_name(bapp.main().context().types(DropboxTransactionBase), sql_trans.type_name)
                assert trans_cls is not None, "Couldn't find transaction's implementation even though it was previously created by us"
                trans = trans_cls(log, sql_instance=sql_trans,
                                       dropbox_finder=self,
                                       package=package,
                                       kvstore=self.merged_kvstore(trans_cls, db))
                log.info("Queuing approved transaction %s", trans)
                sql_trans.set_queued().commit()
                self._ops_queue.put(trans.apply)
            elif auth_token is PT.AUTH_FAILURE:
                log.debug("Resetting invalid authentication login name")
                sql_trans.approved_by_login = sql_trans.TO_BE_APPROVED_MARKER
                session.commit()
            elif auth_token is PT.AUTH_REJECTED:
                # Just cancel the transaction.
                # Usually, can_enqueue implementations will prevent themselves to be executed if a previous
                # package of their type was rejected.
                sql_trans.cancel()
                session.commit()
            else:
                log.debug("Skipping unapproved transaction %s", sql_trans)
            # end handle approval
        # end for each transaction

    ## -- End Thread Tasks -- @}


    # -------------------------
    ## @name Task Schedulers
    # @{

    def _may_queue_tasks(self, id):
        """@return True if we are allowed to queue tasks.
        This is true if the queue is not too full
        @param id string identifying our operation """
        current_queue_size = self._update_queue.qsize()
        if current_queue_size and current_queue_size > self.MAX_UPDATE_QUEUE_SCHEDULE_TASKS:
            log.warn("Skipping %s scheduling as update queue contains more than %i tasks (%i) - IO seems to be overwhelmed", id, self.MAX_UPDATE_QUEUE_SCHEDULE_TASKS, current_queue_size)
            return False
        # end
        return True
        
    def _schedule_dropbox_update(self):
        """Schedule an update - this really is the most 'complex' scheduler as we want to be sure to have only
        one of those on the queue"""
        if self._update_dropboxes_scheduled:
            log.warn("Skipping dropbox update as the previous one is still scheduled, running. IO too slow for current update periods")
            return
        # end ignore slow updates

        self._update_dropboxes_scheduled = True
        log.debug("Scheduling dropbox update")
        self._update_queue.put(self._update_dropboxes)

    def _schedule_packages_change_handling(self):
        """Iterate all packages and schedule package change handling"""
        # note: might have to make this iteration (changes to finder in general) threadsafe
        # use keys as the list might change in the meanwhile
        db_keys = self.dropboxes.keys()
        if not self._may_queue_tasks('package change check'):
            return
        # end skip scheduling

        if not db_keys:
            log.warn("Didn't find a single dropbox in search paths")
            return
        # end inform if we have nothing to do

        log.debug("Scheduling package updates")
        for key in db_keys:
            self._update_queue.put((self._handle_packages_diff, [key]))
        # end for each dropbox to handle

    def _schedule_transaction_check(self):
        """place a task which will check if packages can be run"""
        # NOTE: The transaction check will always be queued. Otherwise it would defeat the purpose
        self._update_queue.put(self._check_transactions)

    ## -- End Task Schedulers -- @}

    # -------------------------
    ## @name Interface
    # @{
    
    def run(self):
        """Main loop, which will never finish unless we have an unexpected failure that is"""
        self.name = type(self).__name__

        config = self._config
        result_queue = Queue()

        # SETUP THREADS
        ################
        result_handler = ResultLoggerThread(result_queue)
        workers = list()

        assert config.db.url, "The url must be set to allow storing package data in a central database"
        assert config.authentication.privileged_group, "Privileged group must be set to prevent misuse"

        # trigger single-threaded db creation and assure db can be reached
        self._url = config.db.url
        test_session = PackageSession.new(url=config.db.url)
        test_session.close()
        del test_session

        assert 0 < config.threads.num_update_threads < self.MAX_WORKERS, "Need to set at least one update thread, max %i" % self.MAX_WORKERS
        assert 0 < config.threads.num_operation_threads < self.MAX_WORKERS, "Need to set at least one operation thread, max %i" % self.MAX_WORKERS

        def start_workers(inq, format, count):
            for tid in xrange(count):
                thread = SessionWorkerThread(log, inq, result_queue, url=config.db.url)
                # We are making sure it will not go down, but in case something is very wrong, it's nicer
                # if the process doesn't hang and can be cleaned up.
                thread.daemon = True
                thread.name = format % tid
                log.info("Starting thread %s" % thread.name)
                thread.start()
                workers.append(thread)
            # end for each thread to create
        # end for each update worker    

        result_handler.start()
        start_workers(self._update_queue, "t-check-%i", config.threads.num_update_threads)
        start_workers(self._ops_queue, "t-operation-%i", config.threads.num_operation_threads)



        # Task-database to simplify periodic checking
        schedulers =    [
                            [0, config.check.dropboxes_every.seconds, self._schedule_dropbox_update],
                            [0, config.check.transactions_every.seconds, self._schedule_transaction_check],
                            [0, config.check.packages_every.seconds, self._schedule_packages_change_handling],
                        ]

        # TASK LOOP
        ###########
        while True:
            if self._should_terminate():
                break
            # end handle interrupt requests

            for info in schedulers:
                last_runtime, update_every, schedule_fun = info
                current_time = time()
                if last_runtime == 0 or current_time >= last_runtime + update_every:
                    last_runtime = current_time
                    try:
                        schedule_fun()
                    except Exception:
                        log.error("Scheduler failed- skipping it. Could be a bug or concurrency issue", exc_info=True)
                    # end handle scheduler exceptions
                    info[0] = current_time
                # end perform update
            # end for each scheduler id
            sleep(1.0)
        # end task loop


        # SHUTDOWN
        ############
        for thread in workers:
            thread.cancel()
            log.info("Canceled thread %s", thread.name)
        # end for each thread

        # Wait for threads
        st = time()
        while workers:
            elapsed = time() - st
            log.info("Waiting for workers to shut down ... %i still active after %.02fs", len(workers), elapsed)
            for thread in workers[:]:
                if not thread.is_alive():
                    workers.remove(thread)
                # end worker is down already
            # end for each worker to possibly remove
            sleep(1)
        # end while there are workers to wait for

    ## -- End Interface -- @}

# end class DaemonThread

