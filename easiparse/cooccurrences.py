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

def normalize(term):
    lowercase = term.lower()
    noendlines = lowercase.replace('\n','').replace('\r','')
    stripped = noendlines.strip()
    return stripped

def occurrences_worker(config, notice, newwl):
    """
    Per year occurrence calculator given an NGram
    """
    input = mongodbhandler.connect(config['cooccurrences']['input_db'])
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    
    if len(newwl['content'])<2:
        raise Exception("the whitelist contains only one element, aborting")

    content = notice['TI']
    if 'DE' in notice:
        content += " " + " ".join(notice['DE'])
    if 'AB' in notice:
        content += " " + notice['AB']

    for doublet in itertools.combinations(newwl['content'], 2):
        
        regex1 = re.compile( r"\b%s\b"%doublet[0], re.I|re.M|re.U )
        regex2 = re.compile( r"\b%s\b"%doublet[1], re.I|re.M|re.U )

        if regex1.search(content) is not None and regex2.search(content) is not None:

            doublet_id12 = notice['issue']['PY']\
                +"_"+ sha256( doublet[0].encode( 'ascii', 'replace')).hexdigest()\
                +"_"+ sha256( doublet[1].encode( 'ascii', 'replace')).hexdigest()
                            
            doublet_id21 = notice['issue']['PY']\
                +"_"+ sha256( doublet[1].encode( 'ascii', 'replace')).hexdigest()\
                +"_"+ sha256( doublet[0].encode( 'ascii', 'replace')).hexdigest()

            if output['mongodb'].mongodb.coocmatrix.find_one({'_id':doublet_id12}) is not None:
                output['mongodb'].mongodb.coocmatrix.update(\
                    {'_id': doublet_id12},\
                    {'_id': doublet_id12, '$inc':\
                    {'value': 1}}, upsert=True)
            elif output['mongodb'].mongodb.coocmatrix.find_one({'_id':doublet_id21}) is not None:
                output['mongodb'].mongodb.coocmatrix.update(\
                    {'_id': doublet_id21},\
                    {'_id': doublet_id21, '$inc':\
                    {'value': 1}}, upsert=True)
            else: 
                output['mongodb'].mongodb.coocmatrix.update(\
                    {'_id': doublet_id12},\
                    {'_id': doublet_id12, '$inc':\
                    {'value': 1}}, upsert=True)

                
def main_occurrences(config):
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
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    # cursor of Whitelist NGrams db
    ngramgenerator = newwl.getNGram()
    count = 0
    try:
        while 1:
            count += 1
            ngid, ng = ngramgenerator.next()
            newwl['content']+=ng['edges']['label'].keys()
            #raise StopIteration()
    except StopIteration:
        logging.debug('imported %d forms from the whitelist %s'%(len(newwl['content']), whitelistpath))
     
    occspool = pool.Pool(processes=config['processes'])
    for notice in input.notices.find(timeout=False):
        occspool.apply_async(occurrences_worker, (config, notice, newwl))
    occspool.close()
    occspool.join()

def cooccurrences_worker(config, doublet):
    """
    Per year cooccurrences calculator based on previous occurrences processing
    """
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    doublet_id = doublet[0]["id"] + "_" + doublet[1]["id"]
    coocline = {"_id": doublet_id}

    term_one_record = outputs['mongodb'].mongodb.whitelist.find_one({"_id": doublet[0]["id"]}, {"notices": 1}, timeout=False)
    term_two_record = outputs['mongodb'].mongodb.whitelist.find_one({"_id": doublet[1]["id"]}, {"notices": 1}, timeout=False)

    if len(term_one_record["notices"].keys()) == 0 or len(term_two_record["notices"].keys()) == 0:
        #logging.warning("no record found for cooc processing")
        return

    for year, notices_list_one in term_one_record["notices"].iteritems():
        if year in term_two_record["notices"]:
            coocline[year] = len(set(notices_list_one) & set(term_two_record["notices"][year]))
            
    outputs['mongodb'].save(coocline,'coocmatrix')

def main_cooccurrences(config):
    """
    main cooccurrences processor
    reads a whitelist and push a cooccurrences_worker() to a process pool
    """
    whitelistpath = config['cooccurrences']["whitelist"]["path"]
    logging.debug("loading whitelist from %s (id = %s)"%(whitelistpath, whitelistpath))

    wlimport = Reader('whitelist://'+whitelistpath, dialect="excel", encoding="ascii")
    wlimport.whitelist = whitelist.Whitelist( whitelistpath, whitelistpath )
    newwl = wlimport.parse_file()
    
    coocspool = pool.Pool(processes=config['processes'])
    # cursor of Whitelist NGrams db
    ngramgenerator = newwl.getNGram()
    ngram_list = []
    try:
        while 1:
            ngid, ng = ngramgenerator.next()
            ngram_list += [ng]
    except StopIteration:
        logging.debug("Got list of %d NGrams"%len(ngram_list))

    #N = len(ngram_list)*(len(ngram_list)-1) / 2
    for i, doublet in enumerate(itertools.combinations(ngram_list, 2)):
        doublet_id = doublet[0]["id"] + "_" + doublet[1]["id"]
#        if not (i+1)%100 or i+1==N:
#            logging.debug( "%d (over %d pairs of terms)"%( i+1, N ) )
        if outputs['mongodb'].mongodb.coocmatrix.find_one({"_id": doublet_id}, timeout=False) is None:
            #cooccurrences_worker(config, doublet)
            coocspool.apply_async(cooccurrences_worker, (config, doublet))

    coocspool.close()
    coocspool.join()

def exportcooc(config):
    """
    Basic exporter of cooccurrences processing to files
    """
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    for pair in outputs['mongodb'].mongodb.coocmatrix.find():
        year, ngi, ngj = pair['_id'].split("_")
        cooc = pair['value']
        #for year, cooc in pair.iteritems():
        #    if year=='_id': continue
        #    if cooc<=0: continue
        outputs['coocmatrixcsv'].save("%s,%s,%d,%s\n"%(ngi, ngj, cooc, year))
    #for ngram in outputs['mongodb'].mongodb.whitelist.find():
    #    outputs['exportwhitelistcsv'].save("%s,%s\n"%(ngram['_id'],ngram['label']))
