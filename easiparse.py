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

from easiparse import importer
import yaml
from glob import glob
import pymongo
import codecs
from os.path import join, split

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

#class AsyncParse(threading.Thread):
#    def __init__(self, config, input_path, mongodb, limit=None):
#        threading.Thread.__init__(self)
#        self.config = config
#        self.input_path = input_path
#        self.mongodb = mongodb
#        self.limit = limit

def worker(config, input_path, mongodb, limit=None):
    try:
        isi_file = codecs.open(input_path, "rU", encoding="ascii",\
            errors="replace")
    except Exception, exc:
        logging.error("Error reading file %s"%input_path)
        return

    output_file = codecs.open( join(config['output_path'], split(input_path)[1]),\
        "w+", encoding="ascii", errors="replace")

    subtotal = importer.main(
        isi_file,
        config,
        output_file,
        mongodb,
        limit=limit
    )
    logging.debug("extracted %d matching notices in %s"%(subtotal, isi_file))

if __name__ == "__main__":
    config = yaml.load( open( "config.yaml", 'rU' ) )
    glob_list = glob(config['input_path'])

    mongodb = pymongo.Connection(config['mongo_host'],\
        config['mongo_port'])[config['mongo_db_name']]

    for input_path in glob_list:
        worker(config, input_path, mongodb)
        #reactor.callInThread(worker, config, input_path, mongodb, limit=None)
    #reactor.run()
