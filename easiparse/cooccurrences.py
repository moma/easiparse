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
from hashlib import sha256
import re
from multiprocessing import pool
from tinasoft.data import Reader
from tinasoft.pytextminer import whitelist, corpus, corpora


import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def normalize(term):
    lowercase = term.lower()
    noendlines = lowercase.replace('\n','').replace('\r','')
    stripped = noendlines.strip()
    return stripped

def occurrences_worker(config, ngram):
    
    input = mongodbhandler.MongoDB(config['cooccurrences']['input_db']['mongo_host'],\
        config['cooccurrences']['input_db']['mongo_port'],\
        config['cooccurrences']['input_db']['mongo_db_name'],\
        config['cooccurrences']['input_db']['mongo_login'])
    outputs = output.getConfiguredOutputs(config['cooccurrences'])

    term_occ = ngram
    term_occ["_id"] = ngram['id']

    regex = re.compile( r"\b%s\b"%"|".join(ngram["edges"]['label'].keys()), re.I | re.M | re.U )
#    notices = input.notices.find({ "$or": [ {"AB":{"$regex":regex}}, {"TI":{"$regex":regex}} ] }, timeout=False)

    notices_TI = input.notices.find({"TI":{"$regex":regex}}, {"issue": 1}, timeout=False)
    notices_AB = input.notices.find({"AB":{"$regex":regex}}, {"issue": 1}, timeout=False)

    count_TI = notices_TI.count()
    count_AB = notices_AB.count()

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
    """

    whitelistpath = config['cooccurrences']["whitelist"]["path"]
    logging.debug("loading whitelist from %s (id = %s)"%(whitelistpath, whitelistpath))

    wlimport = Reader('whitelist://'+whitelistpath)
    wlimport.whitelist = whitelist.Whitelist( whitelistpath, whitelistpath )
    newwl = wlimport.parse_file()

    occspool = pool.Pool(processes=config['processes'])
    # cursor of Whitelist NGrams db
    ngramgenerator = newwl.getNGram()
    try:
        while 1:
            ngid, ng = ngramgenerator.next()
#            occspool.apply_async(occurrences_worker, (config, ng))
            occurrences_worker(config, ng)
    except StopIteration:
        occspool.close()
        occspool.join()
        outputs = output.getConfiguredOutputs(config['cooccurrences'])
        # saves ngrams to the whitelist before export
        allngrams = [(ng['id'], ng) for ng in outputs['mongodb'].mongodb.whitelist.find()]
        newwl.storage.insertManyNGram(allngrams)
        # exports ngrams
        outputs['whitelist'].save(newwl)

def cooccurrences_worker(config, doublet):
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    term_one_id = sha256(doublet[0]).hexdigest()
    term_two_id = sha256(doublet[1]).hexdigest()
    doublet_id = term_one_id + "_" + term_two_id
    coocline = {"_id": doublet_id}

    term_one_record = outputs['mongodb'].mongodb.whitelist.find_one({"_id": term_one_id}, {"notices": 1})
    term_two_record = outputs['mongodb'].mongodb.whitelist.find_one({"_id": term_two_id}, {"notices": 1})

    term_notices = {
        doublet[0]: term_one_record["notices"],
        doublet[1]: term_two_record["notices"]
    }

    for year, notices_list_one in term_notices[doublet[0]].iteritems():
        if year in term_notices[doublet[1]]:
            coocline[year] = len(set(notices_list_one) & set(term_notices[doublet[1]][year]))
#    print coocline
    outputs['mongodb'].save(coocline,'coocmatrix')

def main_cooccurrences(config):
    """
    main cooccurrences processor
    """
#    terms_list = open(config['cooccurrences']["whitelist"]["path"],'rU').readlines()
#    terms_list = list(map(normalize, terms_list))
    whitelistpath = config['cooccurrences']["whitelist"]["path"]
    logging.debug("loading whitelist from %s (id = %s)"%(whitelistpath, whitelistpath))

    wlimport = Reader('whitelist://'+whitelistpath)
    wlimport.whitelist = whitelist.Whitelist( whitelistpath, whitelistpath )
    newwl = wlimport.parse_file()
    N = len(terms_list)*(len(terms_list)-1) / 2
    coocspool = pool.Pool(processes=config['processes'])
    # cursor of Whitelist NGrams db
    ngramgenerator = newwl.getNGram()
    ngramid_list = []
    try:
        while 1:
            ngid, ng = ngramgenerator.next()
            ngramid_list += [ngid]

    except StopIteration:
        for i, doublet in enumerate(itertools.combinations(ngramid_list,2)):
            if not (i+1)%100 or i+1==N:
                logging.debug( "%d (over %d pairs of terms)"%( i+1, N ) )
            cooccurrences_worker(config, doublet)
            "coocspool.apply_async(cooccurrences_worker, (config, doublet))
        coocspool.close()
        coocspool.join()