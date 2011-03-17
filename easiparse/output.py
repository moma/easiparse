# -*- coding: utf-8 -*-
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
__author__="elishowk@nonutc.fr"

from os.path import split, join

import codecs
from mongodbhandler import MongoDB

def getConfiguredOutputs(config, currentfilename=None):
    """
    Parses config to fill it with output objects
    """
    outputs = {}
    if  config['output'] is None:
        return outputs

    if 'mongodb' in config['output']:
        outputs['mongodb'] = Mongo(config)
    if 'files' in config['output']:
        outputs['files'] = File(config, currentfilename)

    return outputs

class Output(object):
    def __init__(self, config, *args, **kwargs):
        self.config = config

    def save(self, record, *args, **kwargs):
        pass

class File(Output):
    def __init__(self, config, currentfilename):
        Output.__init__(self, config)
        self.fileobj = codecs.open(\
            join(config['output']['files']['path'],\
                split(currentfilename)[1]),\
            "w+", encoding="ascii", errors="replace")
    def save(self, record_lines):
        for line in record_lines:
            self.fileobj.write( line )

class Mongo(Output):
    def __init__(self, config):
        Output.__init__(self, config)
        if 'mongo_login' in config['output']['mongodb']:
            self.mongodb = MongoDB(\
                config['output']['mongodb']['mongo_host'],\
                config['output']['mongodb']['mongo_port'],\
                config['output']['mongodb']['mongo_db_name'],\
                config['output']['mongodb']['mongo_login'])
        else:
            self.mongodb = MongoDB(\
                config['output']['mongodb']['mongo_host'],\
                config['output']['mongodb']['mongo_port'],\
                config['output']['mongodb']['mongo_db_name'])

    def save(self, record, recordtype):
        self.mongodb[recordtype].update(\
            {"_id":record['_id']},\
            record,\
            upsert=True)