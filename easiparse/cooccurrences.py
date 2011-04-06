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
import itertools
import re
from multiprocessing import pool
from tinasoft.data import Reader
from tinasoft.pytextminer import whitelist


import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def search_subworker(config, content, year, doublet):
    """
    Responsible for matching the pair and incrementing cooccurrences count
    """
    logging.debug("looking for cooc of %s and %s"%(doublet[0]['label'],
        doublet[1]['label']))
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    regex1 = re.compile( r"\b%s\b"%"|".join(doublet[0]['edges']['label'].keys()), re.I|re.M|re.U )
    regex2 = re.compile( r"\b%s\b"%"|".join(doublet[1]['edges']['label'].keys()), re.I|re.M|re.U )

    if regex1.search(content) is not None and regex2.search(content) is not None:
        logging.debug("found a cooc !")
        # will look for both composed ID
        doublet_id12 = year\
            +"_"+ doublet[0]["id"]\
            +"_"+ doublet[1]["id"]
                        
        doublet_id21 = year\
            +"_"+ doublet[1]["id"]\
            +"_"+ doublet[0]["id"]

        if outputs['mongodb'].mongodb.coocmatrix.find_one({'_id':doublet_id12}) is not None:
            outputs['mongodb'].mongodb.coocmatrix.update(\
                {'_id': doublet_id12},\
                {'_id': doublet_id12, '$inc':\
                {'value': 1}}, upsert=True)
        elif outputs['mongodb'].mongodb.coocmatrix.find_one({'_id':doublet_id21}) is not None:
            outputs['mongodb'].mongodb.coocmatrix.update(\
                {'_id': doublet_id21},\
                {'_id': doublet_id21, '$inc':\
                {'value': 1}}, upsert=True)
        else:
            # anyway saves a new cooc line using 'id12' ID
            outputs['mongodb'].mongodb.coocmatrix.save(\
                {'_id': doublet_id12, 'value': 1})

def worker(config, notice, newwl):
    """
    Cooccurrences worker for a notice given a whitelist object
    """
    logging.debug("entering worker with notice %s"%notice['_id'])

    if len(newwl['content'])<2:
        raise Exception("the whitelist contains only one element, aborting")
    # compose content to search into
    content = ""
    if 'TI' in notice:
        content += notice['TI']
    if 'DE' in notice:
        content += " " + " ".join(notice['DE'])
    if 'AB' in notice:
        content += " " + notice['AB']
    # new process pool to search for ngram pairs
    searchpool = pool.Pool(processes=config['processes'])
    for doublet in itertools.combinations(newwl['content'], 2):
        searchpool.apply_async(search_subworker, (config, content,\
            notice['issue']['PY'], doublet))
    searchpool.close()
    searchpool.join()
                
def main(config):
    """
    main occurrences processor
    reads a whitelist and push a occurrences_worker() to a process pool
    """
    whitelistpath = config['cooccurrences']["whitelist"]["path"]
    logging.debug("loading whitelist from %s (id = %s)"%(whitelistpath, whitelistpath))

    wlimport = Reader('whitelist://'+whitelistpath, dialect="excel", encoding="ascii")
    wlimport.whitelist = whitelist.Whitelist( whitelistpath, whitelistpath )
    newwl = wlimport.parse_file()
    newwl['content']=[]
    # cursor of Whitelist NGrams db
    ngramgenerator = newwl.getNGram()
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    try:
        while 1:
            ngid, ng = ngramgenerator.next()
            newwl['content'] += [ng]
            outputs['exportwhitelistcsv'].save("%s,%s\n"%(ngid,ng['label']))
            #raise StopIteration()
    except StopIteration:
        logging.debug('imported %d n-lemmes from the whitelist file %s'\
                %(len(newwl['content']), whitelistpath))
     
    input = mongodbhandler.MongoDB(config['cooccurrences']['input_db'])
    #occspool = pool.Pool(processes=config['processes'])
    for notice in input.notices.find(timeout=False):
        #occspool.apply_async(worker, (config, notice, newwl))
        worker(config, notice, newwl)
    #occspool.close()
    #occspool.join()


def exportcooc(config):
    """
    Basic exporter of the cooccurrences stored to files
    """
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    for pair in outputs['mongodb'].mongodb.coocmatrix.find():
        year, ngi, ngj = pair['_id'].split("_")
        cooc = pair['value']
        outputs['coocmatrixcsv'].save("%s,%s,%d,%s\n"%(ngi, ngj, cooc, year))
