#-*-coding:utf-8-*-
"""
@package bit.reports.io_stat
@brief Gathers information about the performance of your computer's IO facilities

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['IOStatReportGenerator']

import sys
import socket
import tempfile
import random
import mmap
from time import (time, 
                  sleep)

from butility import (Path,
                      size_to_int,
                      int_to_size_string)
from .base import ReportGenerator

from bit.utility import (  ravg,
                           rsum,
                           DistinctStringReducer,
                           TerminatableThread)

import bapp

# ==============================================================================
## @name Utility Types
# ------------------------------------------------------------------------------
## @{

def mb(bytes):
    """@return bytes as megabytes"""
    return bytes / (1024**2)

class StressorTerminatableThread(TerminatableThread):
    """Performs the actual stress test, but checks for interruption to shutdown gracefully"""
    __slots__ = ('config',   # configuration compatible to the ReportGenerator, sanitized
                '_use_mmap' # if True, we will use an mmap for access
                '_generated_file', # path to generated file object, closed or not

                'elapsed_file_generate_read'# time it took to read bytes from source
                'elapsed_file_generate_write'# time it took to write the data
                'elapsed_file_generate',    # time it took to generate the file in seconds (read + write)
                'elapsed_read_volume',        # time it took to read requested volume from file in seconds
                'elapsed_write_volume',       # time it took to re-write the file randomly
                'file_size',                # size of file we actually generated
                'read_volume',              # Amount of bytes randomly read
                'exception'                 # Error thrown if we failed
                )

    def __init__(self, config, use_mmap=False):
        super(StressorTerminatableThread, self).__init__()
        self.config = config
        self._use_mmap = use_mmap
        self._generated_file = None
        self.reset()


    # -------------------------
    ## @name Utilities
    # @{

    def _unique_file_name(self):
        """@return descriptive and quite unique filename"""
        prefix = 'io-test_%s_%s' % (socket.gethostname(), self.name)
        return tempfile.mktemp(prefix=prefix, suffix='.map',dir=str(self.config.output_dir))

    def _write_file_with_size(self, source, destination, size):
        """Write bytes into destination file from the given source. Will duplicate source if needed, in case
        it didn't contain enough bytes
        @param source an open FileObject. If too small, it must support seek
        @param destination a open FileObject to which bytes are written.
        @param size in bytes destination should reach, assuming it is empty
        @return bytes actually read/written
        @note only source will be closed when done !"""
        dbw = 0
        try:
            while dbw != size:
                if self._should_terminate():
                    break
                # end handle abort
                dbw += self._stream_copy(source.read, destination.write, size - dbw, self.config.write_chunk_size)
                if dbw != size:
                    source.seek(0)
                # end rewind source
            # end while we have to copy bytes
            return dbw
        finally:
            source.close()
        # end assure source is closed

    def _random_access(self, source, source_size, volume):
        """Read the given volume from source, using standard seek/read calls.
        @note unfortunately, the mmap implementation doesn't release the GIL. The FS cache will be hit ...
        @return amount of bytes actually read
        @param source open file object from which a mmap will be created
        @param source_size size of the source in bytes
        @param volume amount of bytes to read"""
        vr = 0
        cs = self.config.random_read_chunk_size
        do_write = self.config.random_writes
        assert cs < source_size, "Chunk size must be smaller than the source of the source file"

        # NOTE: we clamp to a possibly smaller size, thus we never read the last chunk.
        # The algorithm will reuse the same set of randomized chunks to safe python CPU time, after all,
        # only during IO we are multi-threaded

        # list of chunk-ids
        chunk_ids = range(min(source_size, volume) / cs)
        # make sure they are not ordered to simulate random access
        random.shuffle(chunk_ids)
        mm = None

        # NOTE: must divide numbers as xrange can only handle integers !
        for offset_multiplier in xrange((volume / source_size) + bool(volume % source_size)):
            if self._use_mmap:
                mm = mmap.mmap(source.fileno(), 0, mmap.MAP_PRIVATE, mmap.PROT_READ|do_write and mmap.PROT_WRITE or 0)
            # end handle mmap creation

            try:
                # Initialize with zero - this is fast enough
                last_data = '\0' * cs
                for cid in chunk_ids:
                    ofs = cid*cs

                    # PERFORM READ
                    ###############
                    st = time()
                    if mm is not None:
                        data = mm[ofs:ofs+cs]
                    else:
                        source.seek(ofs)
                        data = source.read(cs)
                    # end handle mmap
                    self.elapsed_read_volume += time() - st

                    # PERFORM WRITE
                    ###############
                    if do_write:
                        st = time()
                        if mm is not None:
                            mm[ofs:ofs+cs] = last_data
                            mm.flush()
                        else:
                            source.seek(ofs)
                            source.write(last_data)
                            source.flush()
                        # end handle mmap, file
                        self.elapsed_write_volume += time() - st
                    #end handle writes

                    vr += len(data)
                    last_data = data
                    if vr >= volume or self._should_terminate():
                        return vr
                    # end handle abort or done

                # end for each chunk_id
            finally:
                if mm is not None:
                    mm.close()
                # end handle mmap
            # end handle close mmap
        # end for each read pass of the file

        return vr


    def _stream_copy(self, read, write, size, chunk_size):
        """
        Copy a stream up to size bytes using the provided read and write methods, 
        in chunks of chunk_size
        
        @note its much like stream_copy utility, but operates just using methods"""
        dbw = 0                                             # num data bytes written
        
        # WRITE ALL DATA UP TO SIZE
        while True:
            if self._should_terminate():
                return dbw
            # end early abort
            cs = min(chunk_size, size-dbw)
            # NOTE: not all write methods return the amount of written bytes, like
            # mmap.write. Its bad, but we just deal with it ... perhaps its not 
            # even less efficient
            # data_len = write(read(cs))
            # dbw += data_len
            st = time()
            data = read(cs)
            self.elapsed_file_generate_read += time() - st

            data_len = len(data)
            dbw += data_len

            st = time()
            write(data)
            self.elapsed_file_generate_write += time() - st

            if data_len < cs or dbw == size:
                break
            # END check for stream end
        # END duplicate data
        return dbw


    def _cleanup(self):
        """Remove all files created by us"""
        if self._generated_file:
            self._generated_file.close()
            file_path = Path(self._generated_file.name)
            if file_path.isfile():
                file_path.remove()
            # end delete file
            self._generated_file = None
        # end handle gen file
    
    ## -- End Utilities -- @}


    # -------------------------
    ## @name Subclass Interface
    # @{

    def run(self):
        """Perform all operations, regularly checking for interruptions"""
        try:
            try:
                self._generated_file = open(self._unique_file_name(), 'w+b')

                # CREATE DATASET
                ################
                st = time()
                self.file_size = self._write_file_with_size(open(self.config.source_path, 'rb'), self._generated_file, self.config.file_size)
                self.elapsed_file_generate = time() - st

                
                # OPERATE ON DATASET
                ####################
                self._generated_file.close()
                self._generated_file = open(self._generated_file.name, self.config.random_writes and  'rw+b' or 'rb')
                self.read_volume = self._random_access(self._generated_file, self.file_size, self.config.random_read_volume)
            finally:
                self._cleanup()
            # end assure cleanup
        except Exception, err:
            self.exception = err
            raise
        # end keep exceptions

        return 

    
    ## -- End Subclass Interface -- @}


    # -------------------------
    ## @name Interface
    # @{

    def reset(self):
        """Reset all internal counter variables"""
        self.elapsed_file_generate_write = self.elapsed_file_generate_read = 0
        self.elapsed_file_generate = 0
        self.elapsed_read_volume = self.elapsed_write_volume = 0
        self.file_size = self.read_volume = 0
        self.exception = None
        
    
    ## -- End Interface -- @}        

# end class StressorTerminatableThread

## -- End Utility Types -- @}



class IOStatReportGenerator(ReportGenerator, bapp.plugin_type()):
    """Gathers information about a system's IO performance.

    * create a new unique file a defined directory and write it with data from any source (like /dev/urandom, or a pre-made file with high entropy)
    * memory map that file and ready unique chunks in random order. Unmap the file once all chunks were written, and retry. 
      The idea is to try to workaround the hosts FS cache as good as possible.
    * Possibly open the mmap for writing and change the file randomly, writing it every now and then, to increase stress.
    * Do all of the above in X threads and collect some metrics in the process. Those should be gathered in a Report.
    * react to SIGTERM properly and make sure the test cleans up afterwards.

    @note we are assuming that most of the load will be spent in IO, which is multi-threaded. If not, have to use multiprocessing
    """
    __slots__ = ('_error')

    type_name = 'io-stat'

    description = """Effectively a stress test, whose results will be reported. It can be used to test multi-worker 
    scenarios with plenty of random large file reads and writes. It's useful to verify new hardware is working, 
    and can be run on the fileserver itself or by clients who interact with the fileserver via NFS/SMB.
    You should definitely have a look at the various configuration flags to alter the stress level. """

    prec_2s = lambda f: "%.02fs" % f
    prec_2 = lambda f: "%.02f" % f

    report_schema = (   ('worker', str, str, DistinctStringReducer()),
                        ('t_gen_read[s]', float, prec_2s, ravg),
                        ('t_gen_read[MB/s]', float, prec_2, rsum),
                        ('t_gen_write[s]', float, prec_2s, ravg),
                        ('t_gen_write[MB/s]', float, prec_2, rsum),
                        ('t_gen_total[s]', float, prec_2s, ravg),
                        ('t_read_volume[s]', float, prec_2s, ravg),
                        ('t_read_volume[B]', int, int_to_size_string, rsum),
                        ('t_read_volume[MB/s]', float, prec_2, rsum),
                        ('t_write_volume[MB/s]', float, prec_2, rsum),
                        ('error', str, str, DistinctStringReducer()),
                    )

    _schema = ReportGenerator._make_schema(type_name, dict(num_threads=1,   # Amount of threads to use for IO,
                                                               source_path=Path('/dev/urandom'),# Path to source from which to read data
                                                               output_dir=Path, # Directory to which to write our files - this is the one to test
                                                               write_chunk_size='10m', # size in bytes to read when generating the file
                                                               file_size='1g', # Size of file to generate
                                                               random_writes=0, # If True, the test will also alter the file randomly, flushing after each change
                                                               random_read_chunk_size='1m',  # chunk size when reading randomly
                                                               random_read_volume='4g' # amount of volume to read from the file
                                                                ))

    def __init__(self, *args, **kwargs):
        """Initialize our own members"""
        self._error = False
        super(IOStatReportGenerator, self).__init__(*args, **kwargs)

    # -------------------------
    ## @name Utilities
    # @{

    def _sanitize_configuration(self):
        """@return verified and sanitized configuration"""
        config = self.configuration()
        for size_attr in ('write_chunk_size', 'random_read_volume', 'random_read_chunk_size', 'file_size'):
            setattr(config, size_attr, size_to_int(getattr(config, size_attr)))
        # end for each attribute

        assert config.num_threads >= 1, "Must set at least 1 or more workers"
        assert config.output_dir, "Output directory (output_dir) must be set"
        assert config.output_dir.isdir(), "output directory at '%s' must be an accessible directory" % self.output_dir

        return config
        
    
    ## -- End Utilities -- @}

    # -------------------------
    ## @name Interface
    # @{

    def generate(self):
        report = self.ReportType(columns=self.report_schema)
        config = self._sanitize_configuration()
        record = report.records.append
        workers = list()

        def _record_worker_result():
            # poll them, as join will block
            while workers:
                for w in workers[:]:
                    if w.is_alive():
                        continue
                    # end ignore unfinished workers
                    self._error |= w.exception is not None
                    record((    w.name,
                                w.elapsed_file_generate_read,
                                mb(w.file_size / (w.elapsed_file_generate_read or 1)),
                                w.elapsed_file_generate_write,
                                mb(w.file_size / (w.elapsed_file_generate_write or 1)),
                                w.elapsed_file_generate,
                                w.elapsed_read_volume,
                                w.read_volume,
                                mb(w.read_volume / (w.elapsed_read_volume or 1)),
                                w.elapsed_write_volume and mb(w.read_volume / w.elapsed_write_volume) or 0,
                                w.exception))
                    workers.remove(w)
                # end for each worker
                sleep(0.5)
            # end while we have workers to check
            record(report.aggregate_record())
        # end utility
        print >> sys.stderr, self.configuration()
        print >> sys.stderr, "Creating %s dataset, and a %s %s volume, in %i threads" % \
                                                    (int_to_size_string(config.num_threads * config.file_size),
                                                     int_to_size_string(config.num_threads * config.random_read_volume),
                                                     config.random_writes and 'read and write' or 'read',
                                                     config.num_threads)

        use_mmap = config.num_threads == 1
        if use_mmap:
            print >> sys.stderr, "Using mmap in single-threaded mode, hoping to perfectly workaround the system's FS cache"
        # end 
        try:
            for wid in range(config.num_threads):
                worker = StressorTerminatableThread(config, use_mmap = use_mmap)
                workers.append(worker)
                worker.start()
            # end for each worker

            _record_worker_result()
        except KeyboardInterrupt:
            print >> sys.stderr, "Sending cancellation request to all workers"
            for worker in workers:
                worker.cancel()
            # end for each worker

            print >> sys.stderr, "Waiting for workers to finish - they will stop as soon as possible"
            _record_worker_result()
        # end handle SIGTERM

        return report

    def error(self):
        return self._error

    def generate_fix_script(self, report, writer):
        return False
    
    ## -- End Interface -- @}

# end class IOStatReportGenerator  


