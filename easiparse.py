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
import threading
from os.path import join, split

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

class AsyncParse(threading.Thread):
    def __init__(self, config, input_path, mongodb, limit=None):
        threading.Thread.__init__(self)
        self.config = config
        self.input_path = input_path
        self.mongodb = mongodb
        self.limit = limit

    def run(self):     
        total = 0
        number_files = 0
        try:
            isi_file = codecs.open(self.input_path, "rU", encoding="ascii",\
                errors="replace" )
        except Exception, exc:
            logging.error( "Error reading file %s"%self.input_path )
            return
        output_file = codecs.open( join(self.config['output_path'], split(self.input_path)[1]),\
            "w+", encoding="ascii", errors="replace")
        
        subtotal = importer.main(
            isi_file,
            config,
            output_file,
            mongodb,
            limit=self.limit
        )
        total += subtotal
        number_files += 1
        logging.debug("extracted %d matching notices in %s (done %d files, %d total notices)"\
            %(subtotal, isi_file, number_files, total))

if __name__ == "__main__":
    config = yaml.load( open( "config.yaml", 'rU' ) )
    glob_list = glob(config['data_path'])
    
    mongodb = pymongo.Connection(config['mongo_host'],\
        config['mongo_port'])[config['mongo_db_name']]
    
    thread_list=[]
    for input_path in glob_list:
        asyncparser = AsyncParse(config, input_path, mongodb, None)
        asyncparser.daemon=True
        asyncparser.start()
        thread_list += [asyncparser]

    #[logging.debug(parser) for parser in thread_list]
