#-*-coding:utf-8-*-
"""
@package zfs.sql.session
@brief A session implementation specifically for Z-related data

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ZSession']

import logging

from bapp import ApplicationSettingsMixin
from bkvstore import KeyValueStoreSchema

from sqlalchemy.orm import (Session,
                            sessionmaker)
from sqlalchemy import create_engine
from datetime import datetime

from .orm import (ZPool,
                  ZDataset)

log = logging.getLogger('zfs.sql.session')


class ZSession(Session, ApplicationSettingsMixin):
    """A session for objects defined in our ORM. We integrate with the kvstore to obtain database information."""
    __slots__ = ()

    ## The static type we create once, seems to be wanted by sqlalchemy for optimization, so lets go with it
    _main_type = None
    _schema = KeyValueStoreSchema('zdb', {
                                            ## An sqlalchemy compatible url to our zfs statistical database
                                            'hosturl' : str
                                         })


    # -------------------------
    ## @name Interface
    # @{

    @classmethod
    def new(cls, engine = None):
        """Intialize a new session - if there is no engine, we will instantiate one from our configuration data,
         the schema is assumed to exist already.
        @param engine the engine to use, or None
        """
        if engine is None:
            data = cls.settings_value()
            engine = create_engine(data.hosturl)
        # end create engine

        if cls._main_type is None:
            cls._main_type = sessionmaker(class_=cls)
        # end handle main type

        # Assure we have tables
        from .orm import ZSQLBase
        ZSQLBase.metadata.create_all(engine)
        return cls._main_type(bind=engine)

    def sync(self, host, sample_iterator, type_to_sync):
        """Synchronize the state of the database of all entries for the given host so that they match. The sample_iterator
        produces samples from which we instantiate objects as defined in our orm module. Those will either be added or removed
        from the session.

        @param sample_iterator iterator yielding parser samples, see AdaptiveColumnParser and the likes
        @return this instance
        @note the operation is not committed automatically, you will have to do that once you are ready. This makes
        multiple merges more efficient.
        """
        instances = list()
        names = set()
        now = datetime.now()

        # update samples with the given host
        host_sample = ('host', host)

        # GATHER INSTANCES
        ####################
        for sample in sample_iterator:
            # If host is already included, we overwrite it. Shouldn't be the case though
            sample.append(host_sample)
            inst = type_to_sync.new(sample)
            inst.updated_at = now
            names.add(inst.name)
            instances.append(inst)
        # end handle instances

        if not instances:
            log.warn("Didn't find any sample in sample_iterator - refusing to remove everything. Maybe, the data-provider command failed")
            # NOTE: Ideally, there is a flag to turn this off
            return self
        # end handle failure more gracefull

        # FIND DELETIONS
        #################
        # Query all objects that we don't know and delete them
        ex_names = set()
        for (existing_name,) in self.query(type_to_sync.name).filter(type_to_sync.host == host):
            ex_names.add(existing_name)
        # end for each object to delete

        deleted_names = ex_names - names
        if deleted_names:
            cs = 50  # chunk size
            deleted_names = list(deleted_names)
            for cp in xrange(0, len(deleted_names), cs):
                self.bind.execute(type_to_sync.__table__.delete(type_to_sync.__table__.c.name.in_(deleted_names[cp:cp+cs])))
        # end for each sample

        # MERGE ALL OTHERS
        ##################
        # add or update
        for inst in instances:
            self.merge(inst)
        # end add or update each instance

        return self

    def instance_by_url(self, url, force_dataset = False):
        """@return a existing instance matching the given URL
        @param force_dataset if True, a dataset version will be returned even if the URL points to a pool
        @throw ValueError if there is no such instance"""
        zcls = url.is_pool() and ZPool or ZDataset
        if force_dataset:
            zcls = ZDataset
        inst = self.query(zcls).filter(zcls.host == url.host()).filter(zcls.name == url.name()).first()
        if inst is None:
            raise ValueError("url '%s' didn't point to an existing object" % url)
        return inst

        

    ## -- End Interface -- @}
# end class ZSession
