#-*-coding:utf-8-*-
"""
@package zfs.url
@brief A url which identifies the host, pool, and optionally filesystem and snapshot

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []


from urlparse import (
                        SplitResult,
                        parse_qs,
                        _splitnetloc
                     )

from butility import DictObject


# ==============================================================================
## @name Section
# ------------------------------------------------------------------------------
## @{

scheme_chars = set( 'abcdefghijklmnopqrstuvwxyz'
                    'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                    '0123456789'
                    '+-.')

def urlsplit(url, scheme='', allow_fragments=True):
    """Parse a URL into 5 components:
    <scheme>://<netloc>/<path>?<query>#<fragment>
    Return a 5-tuple: (scheme, netloc, path, query, fragment).
    Note that we don't break the components up in smaller bits
    (e.g. netloc is a single string) and we don't expand % escapes.
    @note copied from urlparse module, but removed special cases"""
    key = url, scheme, allow_fragments, type(url), type(scheme)
    netloc = query = fragment = ''
    i = url.find(':')
    if i > 0:
        for c in url[:i]:
            if c not in scheme_chars:
                break
        else:
            scheme, url = url[:i].lower(), url[i+1:]
        # end handle empty loops
    # end have url

    if url[:2] == '//':
        netloc, url = _splitnetloc(url, 2)
    if allow_fragments and '#' in url:
        url, fragment = url.split('#', 1)
    if '?' in url:
        url, query = url.split('?', 1)
    return SplitResult(scheme, netloc, url, query, fragment)

## -- End Section -- @}
    
    

class ZFSURL(object):
    """A URL type able to represent any zfs dataset. Format is:

    zfs://host/pool[/filesystem[@snapshot]]
    """
    __slots__ = (
                    '_url'  # The actual url we are representing
                )

    # -------------------------
    ## @name Protocol
    # @{

    protocol = 'zfs'
    
    ## -- End Protocol -- @}
    
    
    def __init__(self, url):
        """Intiailize this instance with an entity url string"""
        self._url = urlsplit(url)

    # -------------------------
    ## @name Interface
    # @{
    
    @classmethod
    def new(cls, host, pool, filesystem=None, snapshot=None):
        """Intiailize a new instance of this url type
        @param host sufficiently qualified host name keeping the data, like hostname.domain.intern. 
        @param pool the name of the pool on the host, like 'archive' or 'store'
        @param filesystem path to the filesystem, like 'foo', or 'projects/project-name'
        @param snapshot The optional name of the snapshot, like 'pre_install'
        """
        if pool.startswith('/'):
           path = path[1:]
        # end check inputs
        if filesystem:
            if filesystem.startswith('/'):
                filesystem = filesystem[1:]
            if filesystem.startswith(pool):
                filesystem = filesystem[len(pool)+1:]
            # end cut out pool name in filesystem
        # end check inputs
        
        base = '%s://%s/%s' % (cls.protocol, host, pool)
        if filesystem:
            base += '/%s' % filesystem
        if snapshot:
            base += '@%s' % snapshot
        
        return cls(base)

    @classmethod
    def new_from_dataset(cls, host, dataset, as_dataset = True):
        """@return a url for the given host and dataset.
        @param as_dataset if False and if the dataset doesn't contain a slash, a pool is assumed. Otherwise 
        a filesystem url is generated"""
        assert not dataset.startswith('/')
        if as_dataset and dataset.count('/') == 0:
            dataset += '/'
        # end handle pool filesystem
        return cls('%s://%s/%s' % (cls.protocol, host, dataset))

    def is_pool(self):
        """@return True if the URL is pointing to a pool (and not the corresponding filesystem)"""
        return self._url.path.count('/') == 1

    def is_snapshot(self):
        """@return True if we are a snapshot
        @note we are always a filesystem, as each pool """
        return '@' in self._url.path

    def pool(self):
        """@return the pool portion of this url"""
        return self.name().split('/')[0]

    def filesystem(self):
        """@return our filesystem name. Will at least be the pool, as each pool has a corresponding filesystem"""
        fs = self.name()
        if '@' in fs:
            fs = fs.split('@')[0]
        return fs

    def basename(self):
        """@return the basename of the filesystem"""
        return self.filesystem().split('/')[-1]

    def snapshot(self):
        """@return a path identifying the snapshot on the server for use in zfs commands"""
        if '@' not in self._url.path:
            return None
        return self.name()

    def snapshot_name(self):
        """@return the bare name of the snapshot, like 'foo', instead of filesystem@foo
        Returns None if we are not pointing to a snapshot"""
        ss = self.snapshot()
        if ss is None:
            return None
        return ss.split('@')[-1]

    def name(self):
        """@return the entire name of the pool or dataset"""
        name = self._url.path[1:]
        if name.endswith('/'):
            name = name[:-1]
        return name

    def host(self):
        """@return the host portion of our url"""
        return self._url.netloc

    def parent_filesystem_url(self):
        """@return a url to the parent filesystem from this snapshot or non-pool.
        it may be None if this filesystem is a pool filesystem.
        In case of snapshots, the parent filesystem is the one that owns us"""
        if self.is_snapshot():
            return self.new_from_dataset(self.host(), self.filesystem())
        tokens = self.filesystem().split('/')
        if len(tokens) == 1:
            return None
        # end handle pool-filesystems
        return self.new_from_dataset(self.host(), '/'.join(tokens[:-1]))

    def joined(self, name):
        """@return a new ZFSURL instance with the given name joined with the existing path of the URL.
        If this is a snapshot, the snapshot portion will be kept
        @param name a simple or slash-separated name to join, like 'foo', or 'foo/bar'
        @note the current instance will not be altered"""
        if name.endswith('/'):
            name = name[:-1]
        # end cut extra slashes

        name = '%s/%s' % (self.name(), name)
        ss = self.snapshot_name()
        if ss:
            name += '@%s' % ss
        # end handle snapshot
        return self.new_from_dataset(self.host(), name)

    def query_fields(self):
        """@return a DictObject of fields with arbitrary values.
        @note fields are generic meta-data a url. It is up to the respective code to make use of them"""
        return DictObject(dict((k, v[0]) for k,v in parse_qs(self._url.query).items()))
        
    ## -- End Interface -- @}
    
    
    # -------------------------
    ## @name Protocols
    # @{

    def __eq__(self, rhs):
        """#return True if we are the same url"""
        return self._url == rhs._url

    def __ne__(self, rhs):
        """#return True if we are not the same url"""
        return self._url != rhs._url
        
    def __str__(self):
        """@return ourselves as plain url"""
        return self._url.geturl()
        
    def __repr__(self):
        """@return our representation as string version of our constructor code"""
        return '%s("%s")' % (self.__class__.__name__, self._url.geturl())
        
    ## -- End Comparisons -- @}
    
# end class ZFSURL

