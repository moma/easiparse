# -*- coding: utf-8 -*-
#  Copyright (C) 2010 elishowk@nonutc.fr
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

from os.path import exists
import yaml
from pymongo import Connection
from pymongo.master_slave_connection import MasterSlaveConnection
from pymongo.database import Database
MONGODB_PORT = 27017

def connect(config):
    """
    unique or master/slave pool of connections to MongoDB
    """
    if isinstance(config, dict):
        return mongoDBConnection(config)[config['mongo_db_name']]
    elif isinstance(config, list):
        mongo_db_name = config[0]['mongo_db_name']
        master = None
        slaves = []
        for replicConfig in config:
            try:
                if not replicConfig['slave_okay']:
                    master = mongoDBConnection(replicConfig)
                else:
                    slaves += [mongoDBConnection(replicConfig)]
            except Exception, exc:
                logging.error("error connectiong to replica %s"%replicConfig)
                raise Exception(exc)
        if len(slaves)==0:
            return master[config['mongo_db_name']]
        else:
            slaves += [master]
            ms = MasterSlaveConnection(master, slaves)
            return ms[mongo_db_name]

def mongoDBConnection(config):
    """
    Slave aware Connection to a database
    """
    hostname = config['mongo_host']
    #database = config['mongo_db_name']
    if 'mongo_port' in config:
        port = config['mongo_port']
    else:
        port = MONGODB_PORT
    #if 'mongo_login' in config:
    #    passwordfile = config['mongo_login']
    #else:
    #    passwordfile = None
    return Connection(hostname, port, slave_okay=config['slave_okay'])

#def mongoDBDatabase(connection, database_name, passwordfile=None):
#    """
#    returns an authenticated connection to a database
#    """
#    db = Database(connection, database_name)
#    if passwordfile is not None and exists(passwordfile):
#        password = yaml.load(open(passwordfile, 'rU'))
#        self.authenticate(password['mongo_user'], password['mongo_password'])
#    return db

#class MongoDB(Database):
#    """
#    with or without login/password connector to MongoDB
#    """
#    def __init__(self, config):
#        hostname = config['mongo_host']
#        database = config['mongo_db_name']
#        if 'mongo_port' in config:
#            port = config['mongo_port']
#        else:
#            port = MONGODB_PORT
#        if 'mongo_login' in config:
#            passwordfile = config['mongo_login']
#        else:
#            passwordfile = None
#
#        connection = Connection(hostname, port)
#        Database.__init__(self, connection, database)
#        if passwordfile is not None and exists(passwordfile):
#            password = yaml.load(open(passwordfile, 'rU'))
#            self.authenticate(password['mongo_user'], password['mongo_password'])
