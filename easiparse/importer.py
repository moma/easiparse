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
import re
from glob import glob
from multiprocessing import pool
import codecs

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

from easiparse.filters import NoticeRejected, getConfiguredFilters
from easiparse import output

class Record(object):
    def __init__(self, config, lines, recordtype, fieldsdefinition):
        self.config = config
        self.recordtype = recordtype
        self.fields = fieldsdefinition
        self.last_tag = ""
        self._walkLines(lines)
        self.normalize()

    def _walkLines(self, lines):
        multiline = self.config['isi']['multiline']
        for line in lines:
            tag = self.getTag(line)
            if tag == self.config['isi'][self.recordtype]['key']:
                self.__dict__["_id"] = self.getLine(line)
            if tag not in self.fields.keys() and tag != multiline:
                continue
            self.dispatchValidLine(tag, line)

    def normalize(self):
        for tag, rule in self.fields.iteritems():
            if tag not in self.__dict__.keys(): continue
            if type(rule) == str:
                self.__dict__[tag] = rule.join(self.__dict__[tag])

    def clean(self):
        """
        clean unwanted attributes
        """
        self.__delattr__('config')
        self.__delattr__('last_tag')
        self.__delattr__('fields')

    def appendLine(self, tag, line):
        if tag in self.__dict__.keys():
            self.appendToTag(tag, line)
        else:
            self.createTag(tag, line)

    def parseLine(self, tag, line):
        return [line]

    def appendToTag(self, tag, line):
        self.__dict__[tag] += self.parseLine(tag, line)

    def createTag(self, tag, line):
        self.__dict__[tag] = self.parseLine(tag, line)

    def getTag(self, line):
        tag_length = self.config['isi']['tag_length']
        tag = line[: tag_length].strip()
        return tag

    def getLine(self, line):
        tag_length = self.config['isi']['tag_length']
        return line[tag_length:].strip()

    def dispatchValidLine(self, tag, line):
        stripline = self.getLine(line)
        if tag != self.config['isi']['multiline']:
            self.last_tag = tag
            self.appendLine(tag, stripline)
        else:
            self.appendLine(self.last_tag, stripline)

class Issue(Record):
    def __init__(self, config, lines, recordtype, fieldsdefinition):
        Record.__init__(self, config, lines, recordtype, fieldsdefinition)
        self.clean()
        
class Notice(Record):
    def __init__(self, config, lines, recordtype, fieldsdefinition, filters):
        Record.__init__(self, config, lines, recordtype, fieldsdefinition)
        # apply filters only if defined in config
        self.filters = filters
        self.filter()
        self.clean()

    def _walkLines(self, lines):
        """
        Modified version supporting nested subrecords
        """
        multiline = self.config['isi']['multiline']
        if 'subfields' in self.config['isi'][self.recordtype]:
            subfields = self.config['isi'][self.recordtype]['subfields']
        i = 0
        self.total_lines = len(lines)
        while 1:
            if i == self.total_lines: break
            line = lines[i]
            tag = self.getTag(line)
            if tag == self.config['isi'][self.recordtype]['key']:
                self.__dict__["_id"] = self.getLine(line)
                i += 1
                continue
            if tag not in self.fields.keys() and tag != multiline:
                if 'subfields' in self.config['isi'][self.recordtype] and tag in subfields.keys():
                    i = self.appendSubfield(tag, i, lines)
                    continue
                else:
                    i += 1
                    continue
            self.dispatchValidLine(tag, line)
            i += 1

    def appendSubfield(self, tag, i, lines):
        sublines = []
        i += 1
        while 1:
            if i == self.total_lines:
                raise NoticeRejected("reached EOF without closing a Notice")
            subline = lines[i]
            subtag = self.getTag(subline)
            if subtag == self.config['isi'][self.recordtype]['subfields'][tag]['end']:
                subfield = SubRecord(
                                     self.config,
                                     sublines,
                                     tag,
                                     self.config['isi'][self.recordtype]['subfields'][tag]['fields']
                                     ).__dict__
                if tag in self.__dict__.keys():
                    self.__dict__[tag] += [subfield]
                else:
                    self.__dict__[tag] = [subfield]
                i += 1
                return i
            else:
                sublines += [subline]
            i += 1
        return i

    def clean(self):
        self.__delattr__('total_lines')
        self.__delattr__('config')
        self.__delattr__('last_tag')
        self.__delattr__('fields')
        self.__delattr__('filters')

    def filter(self):
        """
        apply configured filters
        """
        for filter in self.filters:
            filter.apply(self.__dict__)


class SubRecord(Record):
    def __init__(self, config, sublines, parenttag, subfieldsdefinition):
        Record.__init__(self, config, sublines, parenttag, subfieldsdefinition)
        self.clean()
        
    def _walkLines(self, sublines):
        multiline = self.config['isi']['multiline']
        for line in sublines:
            tag = self.getTag(line)
            if tag not in self.fields.keys() and tag != multiline:
                continue
            self.dispatchValidLine(tag, line)


def import_file(file_isi, config, outputs):
    """
    Parses, filters, and save
    """
    issue_begin = re.compile(config['isi']['issues']['begin'] + "\s.*$")
    issue_end = re.compile(config['isi']['issues']['end'] + "\s.*$")
    begin_tag = re.compile(config['isi']['notices']['begin'] + "\s.*$")
    end_tag = re.compile(config['isi']['notices']['end'] + "\s.*$")

    if 'limit' in config:
        limit = config['limit']
    else:
        limit = None

    file_lines = []
    issue_lines = []
    in_notice = 0
    in_issue = 0
    save_issue = 1
    close_issue = 0
    issue = None
    total_imported = 0

    filters = getConfiguredFilters(config)

    # itere sur les lignes du corpus
    for line in file_isi:
        if in_notice == 1 and end_tag.match(line) is None:
            # records the notices' lines
            file_lines += [line]
            continue

        if begin_tag.match(line) is not None and in_notice == 0:
            # opens the in_notice flag and starts recording lines
            in_notice = 1
            file_lines = [line]
            continue

        if end_tag.match(line) is not None and in_notice == 1:
            # closes in_notice flag
            in_notice = 0
            file_lines += [line]
            try:
                # closes issue capturing
                in_issue = 0
                # creates objects
                issue = Record(config, issue_lines, 'issues',\
                    config['isi']['issues']['fields'])
                notice = Notice(config, file_lines, 'notices',\
                    config['isi']['notices']['fields'], filters)
                notice.__dict__['issue'] = issue.__dict__
                
                # saves the issue only if it's the first accepted article
                if  save_issue == 1:
                    # closes save_issue flag
                    save_issue = 0
                    # opens close_issue flag
                    close_issue = 1
                    if 'files' in outputs:
                        outputs['files'].save(issue_lines)
                    if 'mongodb' in outputs:
                        outputs['mongodb'].save(issue.__dict__, "issues")
                        
                # saves the notice
                if 'files' in outputs:
                    outputs['files'].save(file_lines)
                if 'mongodb' in outputs:
                    outputs['mongodb'].save(notice.__dict__, "notices")
                total_imported += 1

            except NoticeRejected:
                pass

            if limit is not None and total_imported >= limit:
                if 'files' in outputs:
                    outputs['files'].save(["RE\n"])
                return total_imported

        if issue_begin.match(line) is not None:
            # opens both in-issue and save_issue flags
            in_issue = 1
            save_issue = 1
            # starts recording the issue's lines
            issue_lines = [line]
            continue

        if in_issue == 1:
            # records issue's lines
            issue_lines += [line]
            continue
            
        if issue_end.match(line) is not None and close_issue==1:
            # closes the issue item and the close_issue flag
            if 'files' in outputs:
                outputs['files'].save(["RE\n"])
            close_issue = 0
            continue

    return total_imported

def import_worker(config, input_path):
    isi_file = codecs.open(input_path, "rU", encoding="ascii", errors="replace")
    outputs = output.getConfiguredOutputs(config['importer'], input_path)
    subtotal = import_file(
        isi_file,
        config['importer'],
        outputs
    )
    logging.debug("imported %d matching notices in %s"%(subtotal, isi_file))

def main(config):
    glob_list = glob(config['importer']['input_path'])
    importpool = pool.Pool(processes=config['processes'])
    for input_path in glob_list:
        importpool.apply_async(import_worker, (config, input_path))
        #import_worker(config, input_path)
    importpool.close()
    importpool.join()