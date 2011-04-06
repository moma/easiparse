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
__author__="elishowk@nonutc.fr"

from os.path import split, join
import re
import codecs
from mongodbhandler import MongoDB
from tinasoft.data import Writer, Reader

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def getConfiguredOutputs(config, currentfilename=None):
    """
    Parses the config to fill it with output objects
    """
    outputs = {
        'mongodb': Output(config),
        'files': Output(config),
        'whitelist': Output(config),
        'coocmatrixcsv': Output(config),
        'exportwhitelistcsv': Output(config),
        'coocoutput': Output(config),
    }
    
    if  config['output'] is None:
        return outputs

    if 'mongodb' in config['output']:
        outputs['mongodb'] = MongoOutput(config)
    if 'files' in config['output']:
        outputs['files'] = File(config, currentfilename)
    if 'whitelist' in config['output']:
        outputs['whitelist'] = WhitelistOutput(config)
    if 'coocmatrixcsv' in config['output']:
        outputs['coocmatrixcsv'] = CoocMatrixCsv(config)
    if 'exportwhitelistcsv' in config['output']:
        outputs['exportwhitelistcsv'] = ExportWhitelistCsv(config)
    if 'coocoutput' in config['output']:
        outputs['coocoutput'] =  CoocOutput(config)
    return outputs


class Output(object):
    """
    abstract class
    """
    def __init__(self, config, *args, **kwargs):
        self.config = config

    def save(self, record, *args, **kwargs):
        pass


class File(Output):
    """
    file output
    """
    def __init__(self, config, currentfilename):
        Output.__init__(self, config)
        self.fileobj = codecs.open(\
            join(config['output']['files']['path'],\
                split(currentfilename)[1]),\
            "w+", encoding="ascii", errors="replace")
    def save(self, record_lines):
        for line in record_lines:
            self.fileobj.write( line )


class CoocMatrixCsv(Output):
    """
    csv file output for coocmatrix
    """
    def __init__(self, config):
        Output.__init__(self, config)
        self.fileobj = codecs.open(\
            config['output']['coocmatrixcsv'],\
            "w+", encoding="ascii", errors="replace")
    def save(self, line):
        self.fileobj.write( line )


class ExportWhitelistCsv(CoocMatrixCsv):
    def __init__(self, config):
        Output.__init__(self, config)
        self.fileobj = codecs.open(\
            config['output']['exportwhitelistcsv'],\
            "w+", encoding="ascii", errors="replace")


class MongoOutput(Output):
    """
    mongo db output class
    """
    def __init__(self, config):
        Output.__init__(self, config)
        self.mongodb = MongoDB(config['output']['mongodb'])

    def save(self, record, recordtype):
        self.mongodb[recordtype].update(\
            {"_id":record['_id']},\
            record,\
            upsert=True)


class WhitelistOutput(Output):
    """
    tinasoft whitelist output : to test
    """
    def __init__(self, config):
        Output.__init__(self, config)

    def save(self, whitelistobj):
        wlexporter = Writer("whitelist://"+self.config['output']['whitelist']['path'])
        wlexporter.write_whitelist(whitelistobj, None, status="w")


class CoocOutput(Output):
    """
    on the fly cooc calculation, given a notice and a whitelist
    """
    def __init__(self, config):
        Output.__init__(self, config)
        self.mongodb = MongoDB(config['output']['coocoutput'])
        self.outputs = getConfiguredOutputs(config['cooccurrences'])
        self._importwhitelist()

    def _importwhitelist(self):
        """
        loads and cache all ngrams in the whitelist
        """
        logging.debug("loading whitelist from %s (id = %s)"%(whitelistpath, whitelistpath))
        whitelistpath = config['cooccurrences']["whitelist"]["path"]
        wlimport = Reader('whitelist://'+whitelistpath, dialect="excel", encoding="ascii")
        wlimport.whitelist = whitelist.Whitelist( whitelistpath, whitelistpath )
        self.newwl = wlimport.parse_file()
        
        try:
            self.newwl['content']=[]
            # cursor of Whitelist NGrams db
            ngramgenerator = newwl.getNGram()
            while 1:
                ngid, ng = ngramgenerator.next()
                self.newwl['content'] += [ng]
                self.outputs['exportwhitelistcsv'].save("%s,%s\n"%(ngid,ng['label']))
                #raise StopIteration()
        except StopIteration:
            logging.debug('imported %d n-lemmes from the whitelist file %s'\
                    %(len(self.newwl['content']), whitelistpath))
        if len(self.newwl['content'])<2:
            raise Exception("the whitelist contains only one element, aborting")


    def search_subworker(self, content, year, doublet):
        """
        Responsible for matching the pair and incrementing cooccurrences count
        """
        logging.debug("looking for cooc of %s and %s"%(doublet[0]['label'], doublet[1]['label']))
        regex1 = re.compile( r"\b%s\b"%"|".join(doublet[0]['edges']['label'].keys()), re.I|re.M|re.U )
        regex2 = re.compile( r"\b%s\b"%"|".join(doublet[1]['edges']['label'].keys()), re.I|re.M|re.U )

        if regex1.search(content) is not None and regex2.search(content) is not None:
            logging.debug("found a cooc !")
            # will look for both composed 
            doublet_id12 = year\
                +"_"+ doublet[0]["id"]\
                +"_"+ doublet[1]["id"]

            doublet_id21 = year\
                +"_"+ doublet[1]["id"]\
                +"_"+ doublet[0]["id"]

            if self.mongodb.coocmatrix.find_one({'_id':doublet_id12}) is not None:
                self.mongodb.coocmatrix.update(\
                    {'_id': doublet_id12},\
                    {'_id': doublet_id12, '$inc':\
                    {'value': 1}}, upsert=True)
            elif self.mongodb.coocmatrix.find_one({'_id':doublet_id21}) is not None:
                self.mongodb.coocmatrix.update(\
                    {'_id': doublet_id21},\
                    {'_id': doublet_id21, '$inc':\
                    {'value': 1}}, upsert=True)
            else:
                # anyway saves a new cooc line using 'id12' ID
                self.mongodb.coocmatrix.save(\
                    {'_id': doublet_id12, 'value': 1})

    def save(self, notice):
        """
        Cooccurrences worker for a notice given a whitelist object
        """
        # compose content to search into
        content = ""
        if 'TI' in notice:
            content += notice['TI']
        if 'DE' in notice:
            content += " " + " ".join(notice['DE'])
        if 'AB' in notice:
            content += " " + notice['AB']
        for doublet in itertools.combinations(self.newwl['content'], 2):
            self.search_subworker(content, notice['issue']['PY'], doublet)