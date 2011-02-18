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

import logging
import re
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")
import exceptions

def record_to_file(output_file, notice_lines):
    """
    Copies record to output file
    """
    if output_file is not None:
        for line in notice_lines:
            output_file.write(line)

def record_to_mongodb(mongodb, record):
    """
    Copies record to mongodb
    """
    if mongodb is not None:
        mongodb[record.recordtype].update({"_id":record.__dict__['_id']}, record.__dict__, upsert=True)

class NoticeRejected(exceptions.Exception):
    pass

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
    def __init__(self, config, lines, recordtype, fieldsdefinition):
        Record.__init__(self, config, lines, recordtype, fieldsdefinition)
        # apply filters only if defined in config
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

    def filter(self):
        """
        apply configured filters
        TODO : filter names call methods
        summary : search for an expression into the required fields of notice
        """
        # no filter at all
        if 'filters' not in self.config or self.config['filters'] is None: return 1

        if 'required_fields' in self.config['filters']:
            
            required_fields = self.config['filters']['required_fields']
            for tag in required_fields:
                if tag not in self.__dict__:
                    raise NoticeRejected("notice incomplete")
                    return 0

        if 'regexp_content' in self.config['filters']:
            match_regexp = re.compile( self.config['filters']['regexp_content']['regexp'] )
            extraction_fields = self.config['filters']['regexp_content']['fields']
            for tag in extraction_fields:
                if tag not in self.__dict__: continue
                if type(self.__dict__[tag]) == str or type(self.__dict__[tag]) == unicode:
                    if match_regexp.search(self.__dict__[tag]) is not None:
                        return 1
                elif type(self.__dict__[tag]) == list:
                    for field in self.__dict__[tag]:
                        if match_regexp.search(field) is not None:
                            return 1
        # anyway : reject
        raise NoticeRejected("notice did not match")
        return 0


class SubRecord(Record):
    def __init__(self, config, sublines, parenttag, subfieldsdefinition):
        Record.__init__(self, config, sublines, parenttag, subfieldsdefinition)
        self.clean()
        
    def _walkLines(self, sublines):
        tag_length = self.config['isi']['tag_length']
        multiline = self.config['isi']['multiline']
        for line in sublines:
            tag = self.getTag(line)
            if tag not in self.fields.keys() and tag != multiline:
                continue
            self.dispatchValidLine(tag, line)


def main(file_isi, config, output_file, mongodb):
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
    issue = None
    total_imported = 0

    # itere sur les lignes du corpus
    for line in file_isi:
        if in_notice == 1 and end_tag.match(line) is None:
            file_lines += [line]
            continue

        if begin_tag.match(line) is not None and in_notice == 0:
            # first time will save an issue object
            if in_issue == 1:
                in_issue = 0
                issue = Record(config, issue_lines, 'issues', config['isi']['issues']['fields'])
                record_to_file(output_file, issue_lines)
                record_to_mongodb(mongodb, issue)

            in_notice = 1
            file_lines = [line]
            continue

        if end_tag.match(line) is not None and in_notice == 1:
            in_notice = 0
            file_lines += [line]
            try:
                notice = Notice(config, file_lines, 'notices', config['isi']['notices']['fields'])
                total_imported += 1
                record_to_file(output_file, file_lines)
                notice.__dict__['issue'] = issue.__dict__
                record_to_mongodb(mongodb, notice)

            except NoticeRejected, nr:
                pass

            if limit is not None and total_imported >= limit:
                return total_imported

        if issue_begin.match(line) is not None:
            in_issue = 1
            issue_lines = [line]
            continue

        if in_issue == 1:
            issue_lines += [line]
            continue
        if issue_end.match(line) is not None:
            # closes the issue item
            record_to_file(output_file, ["RE\n"])
            continue

    return total_imported
