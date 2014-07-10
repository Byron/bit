#-*-coding:utf-8-*-
"""
@package itool.fsstat_schema
@brief Contains the Schema of the file system info database

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from sqlalchemy import (
                            MetaData,
                            Table,
                            Column,
                            String,
                            Integer,
                            BigInteger,
                            Float,
                            SmallInteger,
                            ForeignKey,
                            DateTime,
                            LargeBinary,
                       )

meta = MetaData()
record = Table('fsitem', meta,
                # We create the indices after the fact as it is faster (less IOPs)
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('path', String(2000)), # Must be 1000 to not be too large for an index
                # NOTE: Want to use big integer ! But can't unless it's SQLAlchemy 0.6
                # FIX: change the type of that column once it has been created.
                Column('size', BigInteger),
                Column('atime', DateTime, nullable=True),
                Column('ctime', DateTime, nullable=True),
                Column('mtime', DateTime, nullable=True),
                Column('uid', Integer, nullable=True),
                Column('gid', Integer, nullable=True),
                # Amount of blocks the file occupies on disk.
                # Can be used to compute storage block-overhead
                Column('nblocks', Integer, nullable=True),
                # number of hard links, or subdirectories within a directory
                Column('nlink', Integer, nullable=True),
                # the standard linux file mode
                Column('mode', Integer, nullable=True),
                # The destination of a sylink, or null
                Column('ldest', String(312), nullable=True),
                # SHA will be NULL if we are seeing a symlink
                Column('sha1', LargeBinary(length=20)),
                # Compression ration - the higher the better
                Column('ratio', Float, nullable=True),
                
                # MYSQL Options
                mysql_engine='MyISAM',
                mysql_charset='utf8'
                )

