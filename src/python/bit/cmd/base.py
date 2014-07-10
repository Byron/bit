#-*-coding:utf-8-*-
"""
@package bit.cmd.base
@brief Some basic types for use in all IT commands

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['OverridableSubCommandMixin']

from bcmd import InputError

class OverridableSubCommandMixin(object):
    """A command which uses a KVStore to read its arguments from.

    It allows overriding those arguments using the commandline.
    """
    __slots__ = ()

    def setup_argparser(self, parser):
        try:
            super(OverridableSubCommandMixin, self).setup_argparser(parser)
        except AttributeError:
            # It can be that super doesn't work here as we are not called through a direct instance method, but like
            # OverridableSubCommandMixin.foo(self)
            pass
        # end 

        help = 'A do-nothing argument which is to allow the wrapper to extract context information from the given path.'
        help += 'That in turn will provide particular configuration'
        parser.add_argument('-l', '--location', dest='location', nargs='+', metavar='ABSOLUTE_PATH', help=help)

        help = 'Override any configuration value, relative to the report you are using.'
        help += 'Example: -s foo=bar --set limit=1.24'
        parser.add_argument('-s', '--set', dest='overrides', nargs='+', metavar='key=value', help=help)

        return self


    # -------------------------
    ## @name Subclass Interface
    # @{
    
    def apply_overrides(self, schema, overrides):
        """Parse overrides and set them into a new environments
        @param schema KeyValueStoreSchema of the report we are handling
        @param all override values as 'key=value' string
        @note to be called in execute() method"""
        if not overrides:
            return
        # end early bailout

        env = self.application().context().push('user overrides')
        kvstore = env._kvstore
        for kvstring in overrides:
            tokens = kvstring.split('=')
            if len(tokens) != 2:
                raise InputError("format of user-override is 'key=value', got '%s'" % kvstring)
            #end verify format
            key, value = tokens
            if value.startswith('['):
                try:
                    value = eval(value)
                except Exception:
                    raise InputError("Failed to parse '%s' as a list" % value)
                # end handle conversion
            # end handle 
            kvstore.set_value('%s.%s' % (schema.key(), key), value)
        # end for each string to apply

    ## -- End Subclass Interface -- @}

# end class OverridableSubCommandMixin

