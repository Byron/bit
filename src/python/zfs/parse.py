#-*-coding:utf-8-*-
"""
@package zfs.parse
@brief Various parsers for zfs commandline output

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['MachineColumnParserBase', 'AdaptiveColumnParser', 'ZFSListParser', 'ZFSListSnapshotParser', 
           'ZPoolOmniOSParser', 'ZPoolSolarisParser', 'ZPoolOmniOSLegacyParser', 'ZPoolOmniOSLatestVersionParser']

import logging
from butility import size_to_int
from bit.utility import (date_string_to_datetime,
                         ratio_to_float,
                         bool_label_to_bool)

log = logging.getLogger('zfs.parse')


# -------------------------
## @name Utilities
# @{

## Values we generally consider to be NULL
null_values = set(('-', 'none'))

def next_token(line, start_at):
    """@return iterator yielding (token, token_start, token_end) tuple, where token_start can be None if end of line.
    token_end is one past the last index"""
    ll = len(line)
    if start_at >= ll:
        return None, None, None
    # end end of line check

    val = ''
    cid = 0
    for cid in xrange(start_at, ll):
        c = line[cid]
        if c == ' ':
            if val:
                return val, val_cid, cid + 1
            # end swap last val
        else:
            if not val:
                val_cid = cid
            val += c
        # end handle token type
    # end for each character
    return val, val_cid, cid + 1

def int_or_boolean(string):
    """@return integer or 0|1 depending on whether string is an integer or a boolean"""
    try:
        return int(string)
    except ValueError:
        return int(bool_label_to_bool(string))
    # end handle values

# end token iterator

## -- End Utilities -- @}


class MachineColumnParserBase(object):
    """A parser which can deal with output of zfs commands that have a -H flag.

    -H produces tab -separated lines, one record per line.
    We can deal with size fields and convert them to bytes
    """
    __slots__ = ()

    TOKEN_SEPARATOR = '\t'

    # -------------------------
    ## @name Configuration
    # @{

    ## list of name, converter pairs. converter is a function to create the desired type from the input string 
    ## If converters return None, the entire field is dropped
    ## Must be set by subclass
    schema = None

    ## All column indices that are to be discarded, can be empty, 0 based
    discarded_columns = tuple()

    ## -- End Configuration -- @}

    # -------------------------
    ## @name Interface
    # @{

    def parse_stream(self, reader):
        """@return an iterator yielding a list of name, value pairs, in order of occurrence in line, until reader is
        depleted
        @param reader an iterator yielding one line after another for parsing, newlines are stripped automatically
        @note expects obtain the schema from self.schema"""
        schema = self.schema
        discarded = self.discarded_columns
        assert schema is not None, "schema must be set by subclass"

        for line in reader:
            tokens = line.strip().split(self.TOKEN_SEPARATOR)
            assert len(tokens) - len(discarded) == len(schema), "Schema mismatch"
            converted = list()
            offset = 0
            for tid, token in enumerate(tokens):
                if tid in discarded:
                    offset += 1
                    continue
                # end handle dropped columns
                name, converter = schema[tid-offset]
                if token in null_values:
                    value = None
                else:
                    value = converter(token)
                # end convert value
                converted.append((name, value))
            # end for each tid
            yield converted
        # end for each line
    
    ## -- End Interface -- @}

# end class ZFSParserBase


class AdaptiveColumnParser(object):
    """A parser which can read zfs-list -o all output, which parses columns by judging whitespace between them.

    This also means we can parse the schema dynamically, and provide it, thus there is no need for subclasses
    to specify it anymore. This also implies that we parse highly human readable output.
     """
    __slots__ = (
                    ## similar to a MachineColumnParserBase.schema
                    # None unless parse was called at least once
                    'schema',
                    ## Schema with even more column information
                    '_internal_schema'
                )

    # -------------------------
    ## @name Configuration
    # @{

    ## A mapping of an attribute name to the data-type/constructor
    # We can parse subcolumns, in case a value consists of multiple space-separated columns
    # If not listed here, it's a string. Tuple as value is (converter, num_sub_columns)
    type_map = {
                    # DATASET ATTRIBUTES
                    #####################
                    'creation' : (date_string_to_datetime, 5),
                    'used' : (size_to_int, 1),
                    'avail' : (size_to_int, 1),
                    'refer' : (size_to_int, 1),
                    'ratio' : (ratio_to_float, 1),
                    'mounted' : (bool_label_to_bool, 1),
                    'quota' : (size_to_int, 1),
                    'reserv' : (size_to_int, 1),
                    'volsize' : (size_to_int, 1),
                    'volblock' : (size_to_int, 1),
                    'recsize' : (size_to_int, 1),
                    'atime' : (bool_label_to_bool, 1),
                    'devices' : (bool_label_to_bool, 1),
                    'exec' : (bool_label_to_bool, 1),
                    'setuid' : (bool_label_to_bool, 1),
                    'rdonly' : (bool_label_to_bool, 1),
                    'zoned' : (bool_label_to_bool, 1),
                    'canmount' : (bool_label_to_bool, 1),
                    'xattr' : (bool_label_to_bool, 1),
                    'copies' : (int_or_boolean, 1),
                    'version' : (int, 1),
                    'utf8only' : (bool_label_to_bool, 1),
                    'vscan' : (bool_label_to_bool, 1),
                    'nbmand' : (bool_label_to_bool, 1),
                    'refquota' : (size_to_int, 1),
                    'refreserv' : (size_to_int, 1),
                    'usedsnap' : (size_to_int, 1),
                    'usedds' : (size_to_int, 1),
                    'usedchild' : (size_to_int, 1),
                    'usedrefreserv' : (size_to_int, 1),
                    'defer_destroy' : (bool_label_to_bool, 1),
                    'userrefs' : (int, 1),
                    'dedup' : (bool_label_to_bool, 1),
                    'refratio' : (ratio_to_float, 1),
                    'written' : (size_to_int, 1),
                    'lused' : (size_to_int, 1),
                    'lrefer' : (size_to_int, 1),
                    'zfs:priority' : (int, 1),
                    'zfs:status' : (str, 1),
                }
    
    ## -- End Configuration -- @}

    def __init__(self):
        """Initialize this instance"""
        self.schema = self._internal_schema = None

    def _parse_schema(self, line):
        """Parse a schema from the first line of output, and set the schema accordingly.
        @return a our parsed schema, once without and with column counts + absolute width"""
        schema = list()
        schema_cols = list()
        line = line.strip('\n')

        vend = 0
        while True:
            col, vbegin, vend = next_token(line, vend)
            if vend is None:
                break
            col = col.lower()
            conv, nc = self.type_map.get(col, (str, 1))
            schema.append((col, conv))
            _, nvbegin, _ = next_token(line, vend)
            schema_cols.append((col, conv, nc, nvbegin or vend))
        # end for each column
        return schema, schema_cols

    # -------------------------
    ## @name Interface
    # @{

    def parse_schema(self, line):
        """Set our .schema from the given column line and return it self.
        @return this instance
        """
        self.schema, self._internal_schema = self._parse_schema(line)
        return self
    
    def parse_stream(self, reader):
        """@see MachineColumnParserBase.parse_stream
        @note we will assume the first line is the schema unless the schema was already parsed (as triggered by the caller)
        """
        riter = iter(reader)
        if self._internal_schema is None:
            self.parse_schema(riter.next())
        schema = self._internal_schema
        space = ' '

        def parse_value(line, start_at, conv, ncol):
            """@return tuple of (concatenated converted value, value_start, value_end)"""
            val = ''
            val_end = start_at
            for col_id in xrange(ncol):
                next_val, val_start, val_end = next_token(line, val_end)
                assert next_val is not None, "Column count mismatch"
                if val_start is None:
                    val_start = start_at
                if val:
                    val += space
                # end put space back into sub-values
                val += next_val
            # end for each column to parse
            if val in null_values:
                val = None
            else:
                val = conv(val)
            # end handle conversion
            return val, val_start, val_end
        # end value parser

        for line in riter:
            line = line.strip('\n')
            converted = list()
            val_end = 0
            for sid, (col, conv, nc, abswidth) in enumerate(schema):
                value, val_start, val_end = parse_value(line, val_end, conv, nc)

                # Handle columns with no value, and interpret them as None. This is an inconsistency in the Human-readable
                # version, and we have to compare column width to parse it correctly.
                if val_start >= abswidth:
                    # Reset cursor so we get current value again, use None as this value
                    val_end = abswidth
                    value = None
                converted.append((col, value))
            # end for each token
            yield converted
        # end for each line

    ## -- End Interface -- @}
# end class ColumnHeaderParser


class ZPoolSolarisParser(MachineColumnParserBase):
    """A parser for solaris zpool list -o ALL -H listings"""
    __slots__ = ()

    schema = [
                ('name', str),
                ('size', size_to_int),
                ('cap', size_to_int),
                ('altroot', str),
                ('health', str),
                ('guid', str),
                ('version', int),
                ('bootfs', str),
                ('delegation', bool_label_to_bool),
                ('replace', bool_label_to_bool),
                ('cachefile', str),
                ('failmode', str),
                ('listsnaps', bool_label_to_bool),
                ('expand', bool_label_to_bool),
                ('dedupditto', int),
                ('dedup', ratio_to_float),
                ('free', size_to_int),
                ('alloc', size_to_int),
                ('rdonly', bool_label_to_bool),
              ]    

# end class ZPoolSolarisParser


class ZPoolOmniOSLegacyParser(MachineColumnParserBase):
    """A parser for omnios pools that have not been upgraded yet. Use zpool list -o all -H"""
    __slots__ = ()

    schema = ZPoolSolarisParser.schema + [
                                        ('comment', str),
                                        ('expandsz', size_to_int),
                                        ('freeing', size_to_int),
                                        ('feature@async_destroy', bool_label_to_bool),
                                        ('feature@empty_bpobj', bool_label_to_bool),
                                   ]

    

# end class ZPoolOmniOSLegacyParser


class ZPoolOmniOSParser(MachineColumnParserBase):
    """A parser for omnios pools, which have even more attributes. Use zpool list -o all -H"""
    __slots__ = ()

    schema = ZPoolOmniOSLegacyParser.schema + [('feature@lz4_compress', bool_label_to_bool)]
    
# end class ZPoolOmniOSParser


class ZPoolOmniOSLatestVersionParser(MachineColumnParserBase):
    """The latest version of omniOS has considerably more"""
    __slots__ = ()

    schema = ZPoolOmniOSParser.schema + [('feature@multi_vdev_crash_dump', bool_label_to_bool),
                                         ('feature@spacemap_histogram', bool_label_to_bool),
                                         ('feature@extensible_dataset', bool_label_to_bool)]

# end class ZPoolOmniOSLatestVersionParser


class ZFSListParser(MachineColumnParserBase):
    """parse zfs list -H"""
    __slots__ = ()

    schema = [
                ('filesystem', str),
                ('used', size_to_int),
                ('avail', size_to_int),
                ('refer', size_to_int),
                ('mountpoint', str)
              ]

# end class ZFSListParser


class ZFSListSnapshotParser(MachineColumnParserBase):
    """parse zfs list -t snapshot -H [-r <filesystem>]"""
    __slots__ = ()

    schema = [
                ('filesystem', str),
                ('used', size_to_int),
                ('refer', size_to_int),
              ]

    discarded_columns = (2, 4)

# end class ZFSListSnapshotParser
