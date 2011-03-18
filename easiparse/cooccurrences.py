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

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def count_year(notices):
    occ_y = {}
    for notice in notices:
        occ_y.setdefault(notice["issue"]["PY"],[]).append(notice["_id"])
    return occ_y

def remove_endline(string):
    return string.replace('\n','').replace('\r','')

def worker(config):
    """
    main cooccurrences processor
    """
    input = mongodbhandler.MongoDB(config['cooccurrences']['input_db']['mongo_host'],\
        config['cooccurrences']['input_db']['mongo_port'],\
        config['cooccurrences']['input_db']['mongo_db_name'],\
        config['cooccurrences']['input_db']['mongo_login'])

    occ = {}
    nb_occ = {}

    terms_list = open(config['cooccurrences']["whitelist"]["path"],'rU').readlines()
    terms_list = list(map(remove_endline, terms_list))
    #print terms_list

    outputs = output.getConfiguredOutputs(config['cooccurrences'])
    for term in terms_list:
        nb_occ[term] = {}
        regex = re.compile( "\s%s\s"%term, re.I | re.M | re.U) #\b%s\b"%term does not work...???!!!
        notices = input.notices.find({"AB":{"$regex":regex}}, timeout=False)
        occ[term] = count_year(notices)
        for year, notices_id in occ[term].iteritems():
            nb_occ[term][year] = len(notices_id)
    logging.debug( nb_occ )

    N = len(terms_list)*(len(terms_list)-1) / 2
    for i, doublet in enumerate(itertools.combinations(terms_list,2)):
        doublet_id = sha256(doublet[0]).hexdigest() + "_" + sha256(doublet[1]).hexdigest()
        if not (i+1)%100 or i+1==N:
            logging.debug( "%d (over %d paris of terms)"%( i+1, N ) )
        cooc={"_id": doublet_id}
        for year,occ_y in occ[doublet[0]].iteritems():
            if year in occ[doublet[1]]:
                s,t = occ_y, occ[doublet[1]][year]
                cooc["%d"%year] = len(set(s) & set(t))
        outputs['mongodb'].save(cooc,'coocmatrix')
