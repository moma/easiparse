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

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def normalize(term):
    lowercase = term.lower()
    noendlines = lowercase.replace('\n','').replace('\r','')
    stripped = noendlines.strip()
    return stripped

def occurrences_worker(config, term):
    termid = sha256(term).hexdigest()
    input = mongodbhandler.MongoDB(config['cooccurrences']['input_db']['mongo_host'],\
        config['cooccurrences']['input_db']['mongo_port'],\
        config['cooccurrences']['input_db']['mongo_db_name'],\
        config['cooccurrences']['input_db']['mongo_login'])
    outputs = output.getConfiguredOutputs(config['cooccurrences'])

    term_occ = {}
    term_occ["_id"] = termid
    term_occ["label"] = term

    regex = re.compile( r"\b%s\b"%term, re.I | re.M | re.U )
#    notices = input.notices.find({ "$or": [ {"AB":{"$regex":regex}}, {"TI":{"$regex":regex}} ] }, timeout=False)
    notices = input.notices.find({"AB":{"$regex":regex}}, timeout=False)

    occ_year = {}
    for notice in notices:
        occ_year.setdefault(notice["issue"]["PY"],[]).append(notice["_id"])
    term_occ["notices"] = occ_year
    term_occ["occurrences"] = {}
    for year, notices_id_list in term_occ["notices"].iteritems():
        term_occ["occurrences"][year] = len(notices_id_list)
    outputs['mongodb'].save(term_occ,'whitelist')

def cooccurrences_worker(config, doublet):
    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    term_one_id = sha256(doublet[0]).hexdigest()
    term_two_id = sha256(doublet[1]).hexdigest()
    doublet_id = term_one_id + "_" + term_two_id
    coocline = {"_id": doublet_id}

    term_occurrences = {
        doublet[0]: outputs['mongodb'].mongodb.whitelist.find({"_id": term_one_id}, {"occurrences": 1}),
        doublet[1]: outputs['mongodb'].mongodb.whitelist.find({"_id": term_two_id}, {"occurrences": 1})
    }
    print term_occurrences
    for year, occ_y in term_occurrences[doublet[0]].iteritems():
        if year in term_occurrences[doublet[1]]:
            s,t = occ_y, term_occurrences[doublet[1]][year]
            coocline["%d"%year] = len(set(s) & set(t))
#    print coocline
    outputs['mongodb'].save(coocline,'coocmatrix')

def main_occurrences(config):
    """
    main occurrences processor
    """
    terms_list = open(config['cooccurrences']["whitelist"]["path"],'rU').readlines()
    terms_list = list(map(normalize, terms_list))

#    occspool = pool.Pool(processes=config['processes'])
    for term in terms_list:
#        occspool.apply_async(occurrences_worker, (config, term))
        occurrences_worker(config, term)
#    occspool.close()
#    occspool.join()

def main_cooccurrences(config):
    """
    main cooccurrences processor
    """
    terms_list = open(config['cooccurrences']["whitelist"]["path"],'rU').readlines()
    terms_list = list(map(normalize, terms_list))
    N = len(terms_list)*(len(terms_list)-1) / 2
    
#    coocspool = pool.Pool(processes=config['processes'])
    for i, doublet in enumerate(itertools.combinations(terms_list,2)):
        if not (i+1)%100 or i+1==N:
            logging.debug( "%d (over %d pairs of terms)"%( i+1, N ) )
        cooccurrences_worker(config, doublet)
#        coocspool.apply_async(cooccurrences_worker, (config, doublet))
#    coocspool.close()
#    coocspool.join()