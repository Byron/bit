#-*-coding:utf-8-*-
"""
@package zfs.snapshot
@brief A module with helpers to deal with snapshots

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['SnapshotSender']

import logging

from zfs.url import ZFSURL
from zfs.sql import (ZDataset,
                     ZPool)

from butility import (  LazyMixin,
                        DictObject,
                        int_to_size_string)
from bit.reports import Report

from bit.utility import (delta_to_tty_string,
                         float_percent_to_tty_string,
                         ravg, 
                         rsum,
                         DistinctStringReducer)
from datetime import (timedelta,
                      datetime)
from sqlalchemy.orm import object_session
from sqlalchemy import and_

log = logging.getLogger(__name__)

# -------------------------
## @name Utilities
# @{

def ss_index(ss, ssl):
    """@return index of a snapshot that matched ss.snapshot_name in the given snapshot list, or -1
    @param ss a snapshot object to find in ssl
    @param ssl a list of snapshot objects of another filesystem"""
    name = ss.snapshot_name()
    for sid, oss in enumerate(ssl):
        if oss.snapshot_name() == name:
            return sid
    # end for each snapshot
    return -1

def compute_snapshot_effort(snapshots):
    """@return (total_size, transmission_size) for all the given snapshots.
    total_size is the possibly compressed space of all snapshots, and transmission_size the uncompressed size
    @param snapshots snapshot objects to aggregate"""
    if not snapshots:
        return 0, 0
    # end handle snapshots
    if len(snapshots) == 1:
        ss = snapshots[0]
        return ss.used, ss.used * ss.ratio
    # end handle case were there is no delta
    ssize = 0
    tsize = 0
    base = snapshots[0].refer
    for ss in snapshots[1:]:
        ssize += ss.refer - base
        tsize += int((ss.refer - base) * ss.ratio)
        base = ss.refer
    # end for each snapshot
    return ssize, tsize
    
def first_existing_parent(session, fs_url):
    """@return the first parent url that points to a valid object, or None if there is no such thing
    @note assumes that fs_url doesn't already point to a valid object"""
    parent_url = fs_url.parent_filesystem_url()
    while parent_url:
        try:
            return session.instance_by_url(parent_url)
        except ValueError:
            parent_url = parent_url.parent_filesystem_url()
        # end handle exception
    # end while traversing upwards
    return None
    
# end utility

## -- End Utilities -- @}


class SnapshotSender(LazyMixin):
    """A utility type to prepare sending snapshots from one location to another.

    Locations are always indicated by 
    """
    __slots__ = (
                    '_source_fs',
                    '_dest_url',
                    '_dest_fs'
                )

    # -------------------------
    ## @name Configuration
    # @{

    report_schema = (
                        ('seen', timedelta, delta_to_tty_string, ravg),
                        ('source', str, str, DistinctStringReducer()),
                        ('ss_from', str, str, DistinctStringReducer()),          # First snapshot to send
                        ('ss_to_incl', str, str, DistinctStringReducer()),       # Last snapshot that will be sent (inclusive)
                        ('destination', str, str, DistinctStringReducer()),
                        ('fs_exists', bool, bool, ravg),         # True if destination filesystem exists
                        ('ss_to_send', int, int, rsum),       # Amount of snapshots to be sent
                        ('dest_ss_drop', int, int, rsum),       # Amount of snapshots discarded on the receiving side (from most recent one)
                        ('ss_size', int, int_to_size_string, rsum),  # Amount of space needed for all snapshots
                        ('tr_size', int, int_to_size_string, rsum),  # uncompressed size that needs to be transferred
                        ('fs_free', int, int_to_size_string, rsum),     # free space in destination fs right now
                        ('fs_free_after', int, int_to_size_string, rsum),   # free space after transfer
                        ('pool_cap', int, float_percent_to_tty_string, ravg),   # pool cap of destination fs right now
                        ('pool_cap_after', int, float_percent_to_tty_string, ravg),   # pool cap after transfer
                    )
    
    ## -- End Configuration -- @}

    # -------------------------
    ## @name Constants
    # @{

    ## Name of property on our objects - internal DB name is different
    RECEIVE_URL_PROPERTY = 'zfs_receive_url'

    ## A special marker indicating that the destination should be located automatically
    # This is done by using zfs:receive-url or finding matching snapshots. This is a smart combination of 
    # DEST_MODE_SEARCH and DEST_MODE_PROPERTY
    DEST_MODE_AUTO = 'auto'

    ## If set, we will search matching destinations by matching snapshots. There must be only one for the search
    # to be successful
    DEST_MODE_SEARCH = 'search'

    ## Use the filesystem pointed to by the zfs_receive_url property
    DEST_MODE_PROPERTY = 'property'

    ## Indicates we want to replicate the source filesystem in it's entirety
    REPLICATE_MODE = 'replicate'
    ## ... and force the other side to perfectly match the replication
    REPLICATE_MODE_FORCE = 'replicate_force'

    ## Indicates that zfs:receive-url can be inherited. We will only take children into account, also 
    # if they do not explicitly set their zfs:receive-url
    # By default, this is not the case which can be useful when replication streams are used. Even though
    # they work, in conjunction with retention both copies could become incompatible which requires 
    CHILDREN_ONLY_MODE = 'children_only'

    replicate_modes = (REPLICATE_MODE, REPLICATE_MODE_FORCE)
    dest_fs_modes = (DEST_MODE_AUTO, DEST_MODE_SEARCH, DEST_MODE_PROPERTY)

    ## -- End Constants -- @}

    def __init__(self, source_fs, dest_fs_url):
        """Initialize this instance
        @param source_fs filesystem object of the file-system to send
        @param dest_fs_url the destination filesystem url, underlying filesystem may or may not exist"""
        if dest_fs_url.is_snapshot():
            raise ValueError("Destination URL must point to a filesystem, not a snapshot")
        # end handle detination url
        self._source_fs = source_fs
        self._dest_url = dest_fs_url

    @classmethod
    def _find_destination_fs_candidates(cls, source_fs):
        """@return a list of tuples of (url, filesystem_or_none) pairs.
        filesystem is either the filesystem matching the url, or None
        if the url is of a 'to-be-created' filesystem.

        The list is sorted to show the most viable candidates first. The weight is by existing filesystem names that 
        match the one of source_fs, ordered by free space. No free space check was made here.
        Then you will get urls pointing to non-existing filesystems on pools large enough to hold all data.

        Please note that we didn't yet verify the actual space requirements of 

        @param source_fs the filesystem instance for which you want to find a good location to send it to."""
        # Find mathing fileystems, sorted by free space
        session = object_session(source_fs)
        candidates = list()
        urls_seen = set()
        pools_seen = set()
        for fs in session.query(ZDataset).filter(ZDataset.avail != None).\
                    filter(ZDataset.name.like('%%/%s' % source_fs.url().basename())).\
                    filter(ZDataset.name != source_fs.name).\
                    order_by(ZDataset.avail.desc()):
            fs_url = fs.url()
            urls_seen.add(str(fs_url))
            pools_seen.add(str(ZFSURL.new(fs_url.host(), fs_url.pool())))
            candidates.append((fs_url, fs))
        # end for each matching filesytem

        # Find filesystems which are big enough to hold the entire filesystem + snapshots
        # traverse the hierarchy for that
        ssss = list(source_fs.snapshots())
        surl = source_fs.url()
        ssize, tr_size = compute_snapshot_effort(ssss)
        ssize += source_fs.used
        for pool in session.query(ZPool).filter(ZPool.free > ssize).\
                    filter(ZPool.host != source_fs.host).\
                    order_by(ZPool.free.desc()):
            # prefer filesystems that have a matching subpath and see if parts exist
            if str(pool.url()) in pools_seen:
                continue
            # end make sure that we don't put it onto the same pool twice
            url = pool.url().joined(surl.filesystem())
            if str(url) not in urls_seen:
                candidates.append((url, None))
            # end handle duplicates
            # NOTE: We don't have to update the set anymore, as the pool iteration will yield unique names
        # end handle candidates

        return candidates

    @classmethod
    def _dest_by_property(cls, source_fs):
        """@return destination_url by looking at our source properties.
        @throw ValueError if we couldn't find it"""
        # For this one, we don't except inherited properties
        if not source_fs.property_is_inherited(cls.RECEIVE_URL_PROPERTY):
            durl = getattr(source_fs, cls.RECEIVE_URL_PROPERTY)
            if durl:
                return ZFSURL(durl)
        # end handle inheritance
        raise ValueError("Source filesystem at '%s' didn't have the 'zfs:receive-url' filesystem property set" % source_fs.url())

    @classmethod
    def _dest_by_search(cls, source_fs):
        """@return destination_url by performing a search. The latter URL doesn't necessarily point to an
        existing filesystem
        @see _find_destination_fs_candidates()
        @throw ValueError if there wasn't exactly one match"""
        candidates = list()
        for url, fs in cls._find_destination_fs_candidates(source_fs):
            info = cls._dest_info(source_fs, url, dest_fs = fs)
            candidates.append((info.ss_size, info))
        # end for each candidate to investigate
        if not candidates:
            raise ValueError("Couldn't find a single candidate filesystem to send '%s' to" % source_fs.url())
        candidates = sorted(candidates, key = lambda t: t[0])
        _, info = candidates[0]
        return info.dest_url

    def _set_cache_(self, name):
        if name == '_dest_fs':
            try:
                self._dest_fs = object_session(self._source_fs).instance_by_url(self._dest_url)
            except ValueError:
                self._dest_fs = None
            # end it's ok for destination fs to not exist yet
        else:
            return super(SnapshotSender, self)._set_cache_(name)
        #end handle cache name

    @classmethod
    def _dest_info(cls, source_fs, dest_url, dest_fs = None):
        """@param dest_fs can be provided if you know the filesystem matching your dest_url
        @return DictObject matching our report_schema, with all values filled in. Those can be used
        to prepare a report or the actual script to do the job.
        The fields that are not filled in are

        + seen
         - This is relative to the current time, which is controlled by the callee

        Additional fields are

         + dest_fs
           - The destination filesystem object, which implies such a filesystem exists.
             Can be None
         + dest_url
           - The URL the filesystem will be written. This is equivalent to dest_url in our arguments,
             but will be recomputed if it appears to be a parent filesystem.
         + ss_from_inst
           - Instance of the snapshot from which to start sending. Is None if ss_from is None
         + ss_to_inst
           - Instance of last snapshot which should be sent
         """
        res = DictObject()
        assert not dest_url.is_snapshot()
        if dest_url.basename() != source_fs.url().basename():
            dest_url = dest_url.joined(source_fs.url().basename())
        # end handle filebasename constraint
        session = object_session(source_fs)
        if dest_fs is None:
            try:
                dest_fs = session.instance_by_url(dest_url)
            except ValueError:
                pass
            # end handle no such object
        # end allow to pre-set dest fs
        res.dest_url = dest_url
        res.dest_fs = dest_fs

        res.dest_ss_drop = 0

        ssss = list(source_fs.snapshots())  # s.ource s.naps.hots.

        def set_res_all_ss():
            res.ss_from = None
            res.ss_from_inst = None
            res.ss_to_incl = ssss and ssss[-1].snapshot_name() or None
            res.ss_to_inst = ssss and ssss[-1] or None
            res.ss_to_send = len(ssss)
            res.ss_size, res.tr_size = compute_snapshot_effort(ssss)
            # count in the filesystem
            res.ss_size += source_fs.used
            res.tr_size += source_fs.used * source_fs.ratio
        # end utility

        def set_res(ssid):
            ss = ssss[ssid]
            res.ss_from = ss.snapshot_name()
            res.ss_from_inst = ss
            res.ss_to_incl = ssss[-1].snapshot_name()
            res.ss_to_inst = ssss[-1]
            res.ss_to_send = len(ssss) - ssid
            res.ss_size, res.tr_size = compute_snapshot_effort(ssss[ssid:])
        # end utility

        if dest_fs:
            dlss = dest_fs.latest_snapshot()

            # Assuming that the destination as received snapshots already, the latest destination snapshot 
            # should be in our source snapshot set
            # This is the common case
            ssid = -1
            if dlss is not None:
                ssid = ss_index(dlss, ssss)
            # end handle no snapshot

            if ssid < 0:
                # search the common set of snapshots
                # This could mean that a snapshot was deleted on the destination side
                # There could still be a common subset. If not, the destination needs to be overwritten
                dss = list(dest_fs.snapshots())
                dss_set = set(ss.snapshot_name() for ss in dss)
                ssss_set = set(ss.snapshot_name() for ss in ssss)
                common_ss = dss_set & ssss_set
                if common_ss:
                    for sid, ss in enumerate(reversed(ssss)):
                        if ss.snapshot_name() not in common_ss:
                            continue
                        # use the first match
                        # compute ssid in un-reversed array
                        ssid = len(ssss) - sid - 1
                        break
                    # end for each destination snapshot
                    assert ssid > -1
                    set_res(ssid)

                    # Find the common one in dss to compute the amount of dropped ones
                    common_name = ssss[ssid].snapshot_name()
                    for sid, ss in enumerate(reversed(dss)):
                        if ss.snapshot_name() == common_name:
                            res.dest_ss_drop = sid
                            break
                        # end search for id on the other side
                    # end handle 
                else:
                    # there is no common one, this would mean we send all snapshots
                    set_res_all_ss()
                # end handle common
            else:
                set_res(ssid)
            # end do more intense search
        else:
            # Destination is a new filesystem - transfer size will be the sum of the filesystem + all snapshots
            set_res_all_ss()

            # Find the first parent filesystem that actually exists
            dest_fs = first_existing_parent(session, dest_url)
            assert dest_fs, "Couldn't find an existing parent-filesystem for url '%s'" % dest_url
        # end have existing filesystem

        # epilogue - fill in the filesystem specific values
        res.fs_free = dest_fs.avail
        used_size = dest_fs.is_compressed() and res.ss_size or res.tr_size
        res.fs_free_after = dest_fs.avail - used_size
        pool = dest_fs.pool()
        res.pool_cap = pool.cap
        res.pool_cap_after = pool.cap + (float(used_size) / pool.size) * 100

        return res

    @classmethod
    def _append_info_record(cls, rep, now, source_fs, info):
        """Append a new record to rep with all information from source_fs and info based on our report_schema"""
        rec = [now - source_fs.updated_at,
               source_fs.url(),
               info.ss_from,
               info.ss_to_incl,
               info.dest_url,
               info.dest_fs is not None,
               info.ss_to_send, # # ss to send
               info.dest_ss_drop,
               info.ss_size, # ss size
               info.tr_size, # transmission size
               info.fs_free, # fs free now
               info.fs_free_after, # fs free after
               info.pool_cap,
               info.pool_cap_after, # pool cap after
             ]
        rep.records.append(rec)
        
        

    # -------------------------
    ## @name Interface
    # @{

    @classmethod
    def new(cls, source_fs, destination = DEST_MODE_AUTO):
        """Produce a new SnapshotSender, but allow to be flexible on the destination url which can be found 
        heuristically, to find the most suitable spot for synchronization
        @param source_fs filesystem object to synchronize
        @param destination a url string or one of auto, search, property. It can also be a ZFSURL
        @return a now SnapshotSender instance
        """
        durl = destination
        if not isinstance(durl, ZFSURL):
            if durl == cls.DEST_MODE_AUTO:
                try:
                    durl = cls._dest_by_property(source_fs)
                except ValueError:
                    durl = cls._dest_by_search(source_fs)
                # end hanlde auto mode
            elif durl == cls.DEST_MODE_PROPERTY:
                durl = cls._dest_by_property(source_fs)
            elif durl == cls.DEST_MODE_SEARCH:
                durl = cls._dest_by_search(source_fs)
            else:
                durl = ZFSURL(durl)
        # end handle URL type
        return cls(source_fs, durl)

    @classmethod
    def new_from_properties(cls, source_fs):
        """@return a list of SnapshotSender instances, each one per file system at or recursively underneath the
        filesystem pointed to by the source_url, if it has the zfs:receive-url property set to a value that 
        it didn't inherit from it's parent. This allows to generate multiple senders for all viable (i.e. configured)
        filesystems.
        @param source_url a filesystem object to search for attributes. It's descendants will be searched as well.
        """
        out = list()
        def recurse_fs(child, parent_receive_url, may_use_parent_url = False):
            dest_url = child.zfs_receive_url and ZFSURL(child.zfs_receive_url)
            if dest_url and parent_receive_url is not None and dest_url == parent_receive_url:
                dest_url = None
            # end clear out inheritance

            children_only = False
            if dest_url:
                fields = dest_url.query_fields() or dict()
                try:
                    children_only = cls.CHILDREN_ONLY_MODE in fields and int(getattr(fields, cls.CHILDREN_ONLY_MODE))
                except ValueError:
                    log.error("'children_only' flag must either be 0 or 1", exc_info=True)
                # end 
            # end handle dest_url exists

            # do not us this one in children_only mode - we ignore the parent
            if may_use_parent_url or (not children_only and dest_url):
                out.append(cls(child, dest_url or parent_receive_url))
            # end 
            for sub_fs in child.children():
                recurse_fs(sub_fs, dest_url, may_use_parent_url=children_only)
            # end for each sub-filesystem
            return out
        # end utility

        parent = source_fs.parent()
        return recurse_fs(source_fs, parent and parent.zfs_receive_url and ZFSURL(parent.zfs_receive_url))
        # end handle inheritance

    @classmethod
    def report_candidates(cls, source_fs):
        """@return a report with all possible destinations to which source_fs could be send to"""
        rep = Report(cls.report_schema)
        now = datetime.now()
        for url, fs in cls._find_destination_fs_candidates(source_fs):
            info = cls._dest_info(source_fs, url, dest_fs = fs)
            cls._append_info_record(rep, now, source_fs, info)
        # end for each candidate
        # The less we have to send the better
        rep.records = sorted(rep.records, key=lambda r: r[8])
        return rep

    def report(self, report = None):
        """@return a Report instance of what we would do, with all kinds of interesting information.
        @param report if not None, a report previously returned by another SnapshotSender instance. Useful if
        you want to concatenate reports of different snapshots"""
        rep = report or Report(self.report_schema)
        info = self._dest_info(self._source_fs, self._dest_url)
        self._append_info_record(rep, datetime.now(), self._source_fs, info)
        return rep

    def stream_script(self, writer):
        """Generates a script that will send source source_url to the given destination_url. The script will be 
        made to take care of all eventualities automatically.
        @param writer a function taking strings to write
        @return this instance
        """
        # config notes:
        # Allow force - if set, we may forcibly overwrite a filesystem if there is no common snapshot
        # May replicate ... should be default on, but must see how it really works ... 
        # Especially because makes our computation wrong. Or we get replication aware ... 
        # send properties ... ideally, replicate and this one are defined using fs properties ... .
        # ssh settings - will be required on way or another
        info = self._dest_info(self._source_fs, self._dest_url)
        replicate_mode = None
        send_args = ['zfs', 'send']
        pre_run_script = ''

        # NOTE: Can't use replication in most cases as it doesn't allow backup to retain snapshots longer
        # However
        fields = info.dest_url.query_fields()
        if 'sync' in fields:
            if fields.sync not in self.replicate_modes:
                log.error("Ignoring invalid 'sync' value '%s', may be one of %s", fields.sync, ', '.join(self.replicate_modes))
            else:
                replicate_mode = fields.sync
            # end verify sync value
        # end handle sync options

        if replicate_mode:
            send_args.append('-R')
        # end handle replication
        recv_args = ['zfs', 'receive', '-v']
        if replicate_mode == self.REPLICATE_MODE_FORCE:
            recv_args.append('-F')
        # end handle replication mode

        if info.ss_from_inst is info.ss_to_inst:
            if info.ss_from_inst:
                writer("# Can't send '%s' as destination already has this snapshot - nothing to do unless a new source snapshot is created\n"
                                    % (info.ss_from_inst.url()))
            else:
                writer("# There is no snapshot to send")
            # end handle no snapshot
            return
        # end handle just a single snapshot - can't do anything there

        same_host = self._source_fs.host == info.dest_url.host()

        if info.dest_fs is None:
            if info.ss_from is not None:
                writer("# ERROR: %s -> %s: can't have a matching snapshot if there is no destination filesystem yet\n" % (self._source_fs.url(), info.dest_url))
                return self
            # end early abort on error
            # replication stream should be used to by default, send all snapshots
            # TODO: this should be configurable 
            if '-R' not in send_args:
                send_args.append('-R')
            send_args.append(info.ss_to_inst.name)
        else:
            if info.ss_from is None:
                writer("# ERROR: %s -> %s: Would have to destroy destination filesystem without a common snapshot\n" % (self._source_fs.url(), info.dest_url))
                return self
            # end handle no match

            if info.dest_ss_drop:
                dest_snapshot_name = info.dest_url.name() + '@' + info.ss_from_inst.snapshot_name()
                pre_run_script = "zfs rollback -r %s" % dest_snapshot_name
                if not same_host:
                    pre_run_script = 'ssh %s "%s"' % (info.dest_url.host(), pre_run_script)
                # end handle same-host special case
                pre_run_script += " && "
            # end handle drop using rollback
            send_args.extend(['-I', info.ss_from_inst.snapshot_name(), info.ss_to_inst.name])
        # end handle destination exists

        recv_args.append(info.dest_url.filesystem())

        # For testing, just a cheap one
        zfs_send = ' '.join(send_args)
        zfs_recv = ' '.join(recv_args)

        if same_host:
            ssh_fmt = '%s%s | pv | %s\n'
            transport = zfs_recv
        else:
            receive_cmd = "lz4 -d stdin stdout | %s" % zfs_recv
            # TODO transport should be configurable
            transport = 'ssh -c arcfour128 %s "%s" ' % (info.dest_url.host(), receive_cmd)

            # compression could be configurable, but probably is okay like that
            ssh_fmt = '%s%s | pv | lz4 stdin stdout | %s\n'
        # end handle transport

        writer("# For use on %s\n" % self._source_fs.host)
        writer(ssh_fmt % (pre_run_script, zfs_send, transport))

        return self

    ## -- End Interface -- @}


# end class SnapshotSender


