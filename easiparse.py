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
from optparse import OptionParser
import yaml
from glob import glob
import re

from easiparse import importer, output

import pymongo
import codecs
from multiprocessing import pool
import itertools
import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def import_worker(config, input_path):
    try:
        isi_file = codecs.open(input_path, "rU", encoding="ascii",\
            errors="replace")
    except Exception, exc:
        logging.error("Error importing %s : %s"%(input_path,exc))
        return

    outputs = output.getConfiguredOutputs(config['importer'], input_path)

    subtotal = importer.main(
        isi_file,
        config['importer'],
        outputs
    )
    logging.debug("extracted %d matching notices in %s"%(subtotal, isi_file))


def extract_worker(config, fieldname):
    """
    not modular at all...
    copies input db notices matching a regexg to an output db
    """
    input = pymongo.Connection(\
        config['extractor']['input_db']['mongo_host'],\
        config['extractor']['input_db']['mongo_port'])\
        [ config['extractor']['input_db']['mongo_db_name'] ]
    outputs = output.getConfiguredOutputs( config['extractor'] )
    reg = re.compile( config['extractor']['filters']['regexp_content']['regexp'], re.I|re.U|re.M)
    
    for notice in input.notices.find({ fieldname:{"$regex":reg} }, timeout=False):
        outputs['mongodb'].save(notice, "notices")

def cooccurrences_worker(config, year ):
    """
    not modular at all...
    """
    input = pymongo.Connection(config['input_db']['mongo_host']+"/"+config['input_db']['mongo_db_name'])[config['input_db']['mongo_db_name']]
    outputs = output.getConfiguredOutputs(config)
    coocdict = {}
    for doublet in itertools.combinations(open(config["whitelist"]["path"],'rU').readlines(),2):
        coocdict["::".join(doublet)] = input.notices.find({ "issue.PY": year,\
			"TI":{"$regex": re.compile("\b%s\b"%doublet[0], re.I|re.U|re.M)},\
			"TI":{"$regex": re.compile("\b%s\b"%doublet[1], re.I|re.U|re.M)} }).count()
    outputs['mongodb'].save(coocdict, year)

def get_parser():
    parser = OptionParser()
    parser.add_option("-e", "--execute", dest="execute", help="execution action")
    return parser

if __name__ == "__main__":
    parser = get_parser()
    (options, args) = parser.parse_args()
    print options, args
    config = yaml.load( open( "config.yaml", 'rU' ) )

    if options.execute=='import':
        glob_list = glob(config['importer']['input_path'])
        pool = pool.Pool(processes=config['processes'])
        for input_path in glob_list:
            pool.apply_async(import_worker, (config, input_path))
            #import_worker(config, input_path)
        pool.close()
        pool.join()

    if options.execute=='extract':
        # not modular at all...
        pool = pool.Pool(processes=config['processes'])
        for fieldname in config['extractor']['filters']['regexp_content']['fields']:
            pool.apply_async(extract_worker, (config, fieldname))
        pool.close()
        pool.join()


    if options.execute=='cooccurrences':
        print config['cooccurrences']['input_db']['mongo_host'] + '/'+config['cooccurrences']['input_db']['mongo_db_name']
        input = pymongo.Connection(config['cooccurrences']['input_db']['mongo_host'] + '/'+config['cooccurrences']['input_db']['mongo_db_name'])[config['cooccurrences']['input_db']['mongo_db_name']]
		
        allyears=  input.issues.distinct("PY")
        print allyears
#        pool = pool.Pool(processes=config['processes'])
        for year in allyears:
            cooccurrences_worker(config['cooccurrences'], year)
#            pool.apply_async(cooccurrences_worker, (config['cooccurrences'], year))
 #       pool.close()
  #      pool.join()
