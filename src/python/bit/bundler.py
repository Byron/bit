#-*-coding:utf-8-*-
"""
@package bit.bundler
@brief Contains a tool to find versions from a list of enriched paths

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['VersionBundleList', 'VersionBundle', 'Bundler']

import sys
import os
import re
from itertools import chain

import tx
from butility import LazyMixin


class ListAggregatorMeta(tx.MetaBase):
    """A metaclass to automatically generate methods which aggregate information on a list of items.
    The algorithm does so lazily and all at once upon first request. That way, the list is iterated only once"""
    __slots__ = ()

    # -------------------------
    ## @name Configuration
    # @{

    ## Attribute at which we expect to find our configuration
    # Needs to be tuple of 
    AGGREGATOR_ATTR_NAME = '_aggregator'
    
    ## -- End Configuration -- @}

    def __new__(metacls, name, bases, clsdict):
        """Produce the new type"""
        aggregation_info = clsdict.get(metacls.AGGREGATOR_ATTR_NAME)
        assert aggregation_info is not None, "Require aggregation information, please set your _aggregator attribute - can be empty"

        # Setup slots, but just with our particular aggregation info - they are inherited automatically
        slots = list(clsdict.get('__slots__', tuple()))
        slots.extend(info[0] for info in aggregation_info)
        clsdict['__slots__'] = tuple(slots)

        # Merge aggregator list - to-level class must use results of subclasses
        # As we are called for each subclass, we can assume they have aggregated aggregation information
        # This is necessary as our _set_cache_ method just works on the hierarchically closest aggregation info
        base_aggregator = metacls._class_attribute_value(dict(), bases, metacls.AGGREGATOR_ATTR_NAME)
        if base_aggregator:
            aggregation_info = list(chain(base_aggregator, aggregation_info))
            clsdict[metacls.AGGREGATOR_ATTR_NAME] = aggregation_info
        # end handle info aggregation

        return super(ListAggregatorMeta, metacls).__new__(metacls, name, bases, clsdict)
        
# end class ListAggregatorMeta


class ListAggregatorBase(list, LazyMixin):
    """A type to automatically aggregate values based on information in the _aggregator member"""
    __slots__ = ()

    __metaclass__ = ListAggregatorMeta

    # -------------------------
    ## @name Configuration
    # @{

    ## Needs to be tuple of 
    # [0] = attribute name
    # [1] = aggregator function f(prev, new) -> type(prev)(), where prev is previous result, and new is result of [3]
    # [2] = function f(item) -> type(prev)() returning compatible value for use in aggregator
    ## Must be set in subclass
    _aggregator = tuple()
    
    ## -- End Configuration -- @}


    def _set_cache_(self, name):
        """Aggregate values automatically
        @note we faithfully assume that we have slots to assure we are only called for values that make sense.
        Therefore we will never be called again"""
        if len(self) == 0:
            for item in self._aggregator:
                setattr(self, item[0], None)
            # end for each aggregator
            return
        # end handle empty

        if len(self) == 1:
            member = self[0]
            for item in self._aggregator:
                setattr(self, item[0], item[2](member))
            # end for each aggregator
            return
        # end

        member_iterator = iter(self)
        first_member = member_iterator.next()
        values = [item[2](first_member) for item in self._aggregator]
        getitem = self._aggregator.__getitem__

        
        for member in member_iterator:
            for aid in range(len(self._aggregator)):
                info = getitem(aid)
                values[aid] = info[1](values[aid], info[2](member))
            # end for each aggregator
        # end for each member

        # Set result values
        for name, value in zip((info[0] for info in self._aggregator), values):
            setattr(self, name, value)
        # end for each value to set

    # -------------------------
    ## @name Interface
    # @{

    def clear_cache(self):
        """Clear cache to trigger recalculating it on next query.
        Should be done if list changes in the meanwhile
        @note safe to call even if there is no cache
        @return self"""
        for item in self._aggregator:
            try:
                delattr(self, item[0])
            except AttributeError:
                pass
            # end ignore missing attributes
        # end for each aggregation info item
        return self

    ## -- End Interface -- @}
        

    
# end class ListAggregatorBase


class VersionBundleList(ListAggregatorBase):
    """A sorted list of VersionBundle instances which provides some methods to accumulate information about them.

    They are sorted by their version, ascending
    """
    __slots__ = ()

    # Controls available aggregation methods as created by our meta-class
    version_getter = lambda b: b.version
    _aggregator = (('version_max', max, version_getter),
                   ('version_min', min, version_getter))
    del version_getter

# end class VersionBundleList


class VersionBundle(ListAggregatorBase):
    """A sorted list of tuples which contain information about the path.

    Members are tuples like
    * [0] = path
    * [1] = arbitrary meta-data

    Sorting order is ascending by the first item (path) in the contained tuple.
    It can be used to aggregate information about the contained entries, similar to what the VersionBundleList
    can do.
    """
    __slots__ = (
                    'version',   # The version shared by all of our members
                )

    _aggregator = tuple()

    def __new__(cls, version):
        inst = list.__new__(cls)
        inst.version = version
        return inst

    def __init__(self, *args):
        """do nothing"""
        
# end class VersionBundle


class Bundler(object):
    """Create a dictionary of version bundles, which are prefixed with their longest common parent directory"""
    __slots__ = ()

    # -------------------------
    ## @name Configuration
    # @{

    ## A regular expression to find versions
    re_version = re.compile(r"([_/\\-]v)(\d+)([_/\\-\\.])")

    ## type of bundle we create
    BundleType = VersionBundle

    ## type of bundle list we create
    BundleListType = VersionBundleList

    ## -- End Configuration -- @}


    # -------------------------
    ## @name Internal Utilities
    # @{
    
    def _extract_version_span(self, path):
        """@return None or the span at which to find the version id."""
        m = self.re_version.search(path)
        if m:
            return m.span(2)
        # end handle match

    def _prune_entry(self, out, prefix):
        """A function which may prune the entry at prefix based on certain criteria.
        Default implementations discards single versions"""
        vl = out.get(prefix)
        if not vl or len(vl) > 1:
            return
        # end allow unset prefix or well-filled prefix

        if len(vl) == 1 and len(vl.itervalues().next()) < 2:
            del out[prefix]
            return
        # end handle single-version tree

    ## -- End Utilities -- @}


    # -------------------------
    ## @name Subclass Interface
    # @{

    def _keep_prefix(self, prefix):
        """@return True if the given prefix may be kept in the datastructure during rebuild_bundle()
        @note defaults to keep everything"""
        return True

    def _keep_item(self, item):
        """@return True if the given item is supposed to remain in it's bundle
        @note called by _list_to_bundle()"""
        return True

    def _convert_version(self, version):
        """@return the normalized or actual version based on the input version string
        @note default implementation will try to convert the version into an integer, but keep a string
        otherwise"""
        try:
            return int(version)
        except ValueError:
            return version
        # end handle invalid integer

    def _list_to_bundle(self, version, version_list):
        """@return a VersionBundle list as converted from the given list of versions
        @param version the version as returned by _convert_version()
        @param version_list a list of (path, meta) tuples which will all have the same version and prefix"""
        res = self.BundleType(version)
        res.extend(item for item in version_list if self._keep_item(item))
        return res

    def _iter_bundles_in_dict(self, bundle_dict):
        """@return iterator for all bundles in the given dict. It will filter and prune as needed."""
        for k,v in bundle_dict.iteritems():
            bundle = self._list_to_bundle(self._convert_version(k), v)
            if not bundle:
                continue
            yield bundle
        # end for each prefix, bundle list
        
    def _dict_to_bundle_list(self, prefix, bundle_dict):
        """@return a VersionBundleList instance as built from the given bundle_dict.
        We assure the versions are sorted ascending by their converted version value
        @param prefix which is common to all versions of the bundle_dict
        @param bundle_dict as found at the prefix"""
        bundle_list = self.BundleListType()
        bundle_list.extend(self._iter_bundles_in_dict(bundle_dict))
        bundle_list.sort(key=lambda b: b.version)
        return bundle_list
        
    ## -- End Subclass Interface -- @}
        

    # -------------------------
    ## @name Interface
    # @{
    
    def bundle(self, record_iterator):
        """Bundle records provided by record_iterator and build the return value.
        The latter consists of dict at a key which is the common prefix of all contained versioned 
        paths. We never keep unversioned paths, or single versions.
        @param record_iterator yielding tuples of 
        * [0] = path - the path to a file or folder
        * [1] = any kind of meta-data which will remain attached to the corresponding path
        @param options compatible to default_options, accessible using plain getattr
        @return dict of prefixes associated with a dict of version->list((path, metadata)) instances.
        @note the returned value supports marshaling 
        """
        out = dict()
        set_default = out.setdefault
        dirname = os.path.dirname
        extract_span = self._extract_version_span

        cur_prefix = None
        for path, meta in record_iterator:
            sp = extract_span(path)
            if sp:
                prefix = path[:sp[0]]
                version = path[sp[0]:sp[1]]

                bundle_dict = set_default(prefix, {})
                bundle = bundle_dict.setdefault(version, [])

                # either there is no prefix yet, or it matches
                if cur_prefix and prefix != cur_prefix:
                    # finish up the current bundle, start a new one
                    self._prune_entry(out, cur_prefix)
                # end handle prefix change  

                bundle.append((path, meta))
                cur_prefix = prefix
            else:
                # there is no version at all, finish our current prefix_list
                self._prune_entry(out, cur_prefix)
                cur_prefix = None
            # end handle span
        # end for each rec

        return out

    def rebuild_bundle(self, bundle):
        """Take product of bundle() method and rebuild it to better types to help analysing and mining it
        It will call methods to delegate certain decisions to subclasses.
        @param bundle product of bundle() method. It will be changed in place !
        @return the rebuilt bundle, which was changed in place"""
        for prefix in bundle.keys():
            if not self._keep_prefix(prefix):
                del bundle[prefix]
                continue
            # end prune by prefix

            blist = self._dict_to_bundle_list(prefix, bundle[prefix])
            if not any(blist):
                del bundle[prefix]
                continue
            # skip items which are now empty (due to filtering of subclasses)
            bundle[prefix] = blist
        # end for each prefix

        return bundle

    ## -- End Interface -- @}
# end class Bundler
