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

def occurrences_worker(config, ngram):
    """
    Per year occurrence calculator given an NGram
    """
    input = mongodbhandler.MongoDB(config['cooccurrences']['input_db'])
    outputs = output.getConfiguredOutputs(config['cooccurrences'])

    term_occ = ngram
    term_occ["_id"] = ngram['id']

    regex = re.compile( r"\b%s\b"%"|".join(ngram["edges"]['label'].keys()), re.I | re.M | re.U )

    notices_TI = input.notices.find({"TI":{"$regex":regex}}, {"issue": 1}, timeout=False)
    notices_AB = input.notices.find({"AB":{"$regex":regex}}, {"issue": 1}, timeout=False)

    count_TI = notices_TI.count()
    count_AB = notices_AB.count()

    if count_AB==0 and count_TI==0:
        logging.warning("no matching notices")
        return
    else:
        logging.warning("found matching notices for %s"%" or ".join(ngram["edges"]['label'].keys()))

    if count_TI >= count_AB:
        notices = notices_TI
    else:
        notices = notices_AB

    occ_year = {}
    for notice in notices:
        occ_year.setdefault(notice["issue"]["PY"],[]).append(notice["_id"])
    term_occ["notices"] = occ_year
    
    for year, notices_id_list in term_occ["notices"].iteritems():
        term_occ.addEdge( "Corpus", year, len(notices_id_list))

    outputs['mongodb'].save(term_occ.__dict__, 'whitelist')

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

    occspool = pool.Pool(processes=config['processes'])
    # cursor of Whitelist NGrams db
    ngramgenerator = newwl.getNGram()
    count =0
    try:
        while 1:
            count += 1
            ngid, ng = ngramgenerator.next()
            occspool.apply_async(occurrences_worker, (config, ng))
            #occurrences_worker(config, ng)
    except StopIteration:
        logging.debug("start pool processing occurrences of %d ngrams"%count)
        
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
            
    #logging.debug(coocline)
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
#        if not (i+1)%100 or i+1==N:
#            logging.debug( "%d (over %d pairs of terms)"%( i+1, N ) )
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
        ngi, ngj = pair['_id'].split("_")
        for year, cooc in pair.iteritems():
            if year=='_id': continue
            if cooc<=0: continue
            outputs['coocmatrixcsv'].save("%s,%s,%d,%s\n"%(ngi, ngj, cooc, year))
    for ngram in outputs['mongodb'].mongodb.whitelist.find():
        outputs['exportwhitelistcsv'].save("%s,%s\n"%(ngram['_id'],ngram['label']))
