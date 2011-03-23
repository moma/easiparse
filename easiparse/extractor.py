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

import mongodbhandler
import output
import re
from multiprocessing import pool

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def extract_worker(config, fieldname):
    """
    copies input db notices matching a regexg to an output db
    """
    input = mongodbhandler.MongoDB(config['extractor']['input_db']['mongo_host'],\
            config['extractor']['input_db']['mongo_port'],\
            config['extractor']['input_db']['mongo_db_name'],\
            config['extractor']['input_db']['mongo_login'])
    outputs = output.getConfiguredOutputs( config['extractor'] )
    reg = re.compile( config['extractor']['filters']['regexp_content']['regexp'], re.I|re.U|re.M)

    for notice in input.notices.find({ fieldname:{"$regex":reg} }, timeout=False):
        outputs['mongodb'].save(notice, "notices")

def main(config):
    extractpool = pool.Pool(processes=config['processes'])
    for fieldname in config['extractor']['filters']['regexp_content']['fields']:
        extractpool.apply_async(extract_worker, (config, fieldname))
    extractpool.close()
    extractpool.join()