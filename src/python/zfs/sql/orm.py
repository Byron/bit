#-*-coding:utf-8-*-
"""
@package zfs.sql.orm
@brief The SQL schema used for the databases

We use an ORM from which the schema is extracted. It can be used to initialize databases, as well as to use them.
@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZDataset', 'ZPool', 'ZSQLBase']

from sqlalchemy import (
                            Column,
                            BigInteger,
                            String,
                            Integer,
                            DateTime,
                            ForeignKey,
                            Float,
                            Boolean
                        )

from sqlalchemy.ext.declarative import declarative_base
from zfs.url import ZFSURL
from sqlalchemy.orm import object_session

# For now, just myisam, as we have no complex relations. Could be something for pools, as those tables are small.
# lets see ... 
_table_args = dict(mysql_engine = 'MyISAM', mysql_charset = 'utf8')

class ZSQLBase(object):
    """Base class for all SQL related Z-types"""
    __slots__ = ()

    def __str__(self):
        """@return our string representation"""
        return str(self.url())

    # -------------------------
    ## @name Utilities
    # @{

    @classmethod
    def parse_result_to_dict(cls, parse_result):
        """@param parse_result a single result yielded from AdaptiveColumnParser.parse_stream() or 
        MachineColumnParserBase.parse_stream().
        @return a dict with key,value pairs taken from the given parse-result, suitable to be taken 
        into the constructor of our type, like cls(**parse_dict). Column names are expected to match, and those
        that don't will just be discarded"""
        res = dict()
        name_mapper = cls.__mapper__.get_property_by_column
        cols = cls.__table__.columns
        for col, val in parse_result:
            if col not in cols:
                continue
            # end skip unknown
            res[name_mapper(cols[col]).key] = val
        # end for each result
        return res

    @classmethod
    def new(cls, parse_result):
        """@return a new class instance from the given parse result"""
        # Use arguments instead of kwargs, as we have columns which are incompatible.
        # We need them ordered
        return cls(**cls.parse_result_to_dict(parse_result))

    ## -- End Utilities -- @}


    # -------------------------
    ## @name Interface
    # @{

    
    ## -- End Interface -- @}

# end class SQLBase

ZSQLBase = declarative_base(cls = ZSQLBase)


class ZDataset(ZSQLBase):
    """A persistent version of an SQL dataset"""
    __slots__ = ()

    __tablename__  = 'zdataset'
    __table_args__ = _table_args

    host           = Column(String(128), primary_key = True)
    name           = Column(String(256), primary_key = True)
    type           = Column(String(64))
    creation       = Column(DateTime)
    used           = Column(BigInteger)
    avail          = Column(BigInteger)
    refer          = Column(BigInteger)
    ratio          = Column(Float)
    mounted        = Column(Boolean)
    origin         = Column(String(256))
    clones         = Column(String(256))
    quota          = Column(BigInteger)
    reserv         = Column(BigInteger)
    volsize        = Column(BigInteger)
    volblock       = Column(BigInteger)
    recsize        = Column(BigInteger)
    mountpoint     = Column(String(256))
    sharenfs       = Column(String(256))
    checksum       = Column(String(256))
    compress       = Column(String(32))
    atime          = Column(Boolean)
    devices        = Column(Boolean)
    # for now, no exec ! We need remapping at least on the parser side !
    exec_          = Column('exec', Boolean)
    setuid         = Column(Boolean)
    rdonly         = Column(Boolean)
    zoned          = Column(Boolean)
    snapdir        = Column(String(256))
    aclinherit     = Column(String(256))
    canmount       = Column(Boolean)
    xattr          = Column(Boolean)
    copies         = Column(Integer)
    version        = Column(Integer)
    utf8only       = Column(Boolean)
    normalization  = Column(String(32))
    case           = Column(String(32))
    vscan          = Column(Boolean)
    nbmand         = Column(Boolean)
    sharesmb       = Column(String(256))
    refquota       = Column(BigInteger)
    refreserv      = Column(BigInteger)
    primary_key    = Column(String(32))
    secondarycache = Column(String(32))
    usedsnap       = Column(BigInteger)
    usedds         = Column(BigInteger)
    usedchild      = Column(BigInteger)
    usedrefreserv  = Column(BigInteger)
    defer_destroy  = Column(Boolean)
    userrefs       = Column(Integer)
    logbias        = Column(String(64))
    dedup          = Column(Boolean)
    mlslabel       = Column(String(64))
    sync           = Column(String(32))

    # Our own attribute keep track of when the value was generated
    updated_at     = Column(DateTime)

    tx_priority    = Column('tx:priority', Integer)
    tx_receive_url = Column('tx:receive-url', String(256))

    # -------------------------
    ## @name Interface
    # @{

    def is_pool_filesystem(self):
        """@return True if we are the filesystem of a corresponding pool object"""
        return '/' not in self.name

    def is_snapshot(self):
        """@return True if we are a snapshot"""
        return self.avail is None

    def filesystem_name(self):
        """@return the filesystem name. If this is a snapshot, the name will be adjusted accordingly"""
        return self.name.split('@')[0]

    def snapshot_name(self):
        """@return the actual name of the snapshot if this is a snapshot.
        @throw AssertionError otherwise"""
        assert self.is_snapshot()
        return self.name.split('@')[-1]

    def is_compressed(self):
        """@return True if this filesystem has compression enabled and is thus (most likely) to be compressed.
        @note works for snapshots and filesystems"""
        if self.is_snapshot():
            return self.parent().is_compressed()
        return self.compress != 'off'

    def url(self):
        """@return our location as ZFSURL
        @note if this is a pool-filesystem, a / will be appended to indicate that"""
        name = self.name
        if '/' not in name:
            name += '/'
        return ZFSURL.new_from_dataset(self.host, name, True)

    ## -- End Interface -- @}

    # -------------------------
    ## @name Traversal
    # @{

    def parent(self):
        """@return the parent filesystem of this filesystem or snapshot, or None if this is a pool-filesystem"""
        purl = self.url().parent_filesystem_url()
        if purl is None:
            return None
        return object_session(self).instance_by_url(purl)

    def children(self):
        """@return a iterable of all intermediate child filesystems - they have this filesystem as parent"""
        tc = self.name.count('/')
        for inst in object_session(self).query(ZDataset).filter(ZDataset.avail != None).\
                                            filter(ZDataset.host == self.host).\
                                            filter(ZDataset.name.like(self.name + '/%')).\
                                            order_by(ZDataset.creation):
            if inst.name.count('/') - 1 == tc:
                yield inst
            # end prune indirect children
        # end for each filesystem instance

    def snapshots(self):
        """@return an iterable of all snapshots directly underneath this filesystem, from oldest to newest
        @note snapshots don't have snapshots"""
        if self.is_snapshot():
            return tuple()
        # end handle snapshot

        return object_session(self).query(ZDataset).filter(ZDataset.avail == None).\
                                    filter(ZDataset.name.like(self.name + '@%')).\
                                    order_by(ZDataset.creation)

    def latest_snapshot(self):
        """@return the latest, most recent snapshot available for this filesystem. Will be None if this is not
        a filesystem, or if there is no snapshot at all"""
        return object_session(self).query(ZDataset).filter(ZDataset.avail == None).\
                                    filter(ZDataset.name.like(self.name + '@%')).\
                                    order_by(ZDataset.creation.desc()).first()
        
    def as_pool(self):
        """@return the pool instance which corresponds to us, if this is a pool filesystem.
        Otherwise None is returned"""
        return object_session(self).query(ZPool).filter(ZPool.host == self.host).filter(ZPool.name == self.name).first()
    
    def pool(self):
        """@return the pool object that owns this filesystem or snapshot"""
        url = self.url()
        return object_session(self).instance_by_url(ZFSURL.new_from_dataset(url.host(), url.filesystem().split('/')[0], as_dataset=False))

    def property_is_inherited(self, name):
        """@return True if the given property is inherited from the parent. 
        @param name a getattr compatible name of the property to check.
        @note if the respective value is None for self and parent, it will still look inherited
        @note we can only identify this by checking the respective parent value"""
        parent = self.parent()
        if parent is None:
            return False
        # end pool fs never inehrit anything
        return getattr(parent, name) == getattr(self, name)
        
    ## -- End Traversal -- @}

# end class ZDataset


class ZPool(ZSQLBase):
    """A persistent representation of a zpool"""
    __slots__ = ()

    __tablename__ = 'zpool'
    __table_args__ = _table_args

    host       = Column(String(128), primary_key = True)
    name       = Column(String(256), primary_key = True)
    size       = Column(BigInteger)
    cap        = Column(BigInteger)
    altroot    = Column(String(256))
    health     = Column(String(256))
    guid       = Column(String(128))
    version    = Column(Integer)
    bootfs     = Column(String(256))
    delegation = Column(Boolean)
    replace    = Column(Boolean)
    cachefile  = Column(String(256))
    failmode   = Column(String(64))
    listsnaps  = Column(Boolean)
    expand     = Column(Boolean)
    dedupditto = Column(Integer)
    dedup      = Column(Float)
    free       = Column(BigInteger)
    alloc      = Column(BigInteger)
    rdonly     = Column(Boolean)

    # Inform about when the value was last updated
    updated_at = Column(DateTime)


    # -------------------------
    ## @name Interface
    # @{
    
    def url(self):
        """@return our location as ZFSURL"""
        return ZFSURL.new_from_dataset(self.host, self.name, False)

    def as_filesystem(self):
        """@return an sql orm instance which matches this pool's filesystem.
        @note the returned filesystem will return True for is_pool_filesystem()"""
        return object_session(self).query(ZDataset).filter(ZDataset.host == self.host).filter(ZDataset.name == self.name).first()

    ## -- End Interface -- @}

# end class ZPool
