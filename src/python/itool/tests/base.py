#-*-coding:utf-8-*-
"""
@package itool.tests.base
@brief Basic types for testing

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ItoolTestCase']

from butility.tests import TestCase


class ItoolTestCase(TestCase):
    """Base type for all Itool related tests"""
    __slots__ = ()

# end class ItoolTestCase
