# -*- coding: utf-8 -*-
import re
import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")
import exceptions

def notice_to_file(output_file, notice_lines):
    """
    Copies notice lines to output file
    """
    if output_file is not None:
        for line in notice_lines:
            output_file.write( line )

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
        self.clean()

    def _walkLines(self, lines):
        tag_length = self.config['isi']['tag_length']
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
        self.__delattr__('recordtype')
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
        tag = line[ : tag_length ].strip()
        return tag

    def getLine(self, line):
        tag_length = self.config['isi']['tag_length']
        return line[ tag_length : ].strip()

    def dispatchValidLine(self, tag, line):
        stripline = self.getLine(line)
        if tag != self.config['isi']['multiline']:
            self.last_tag = tag
            self.appendLine(tag, stripline)
        else:
            self.appendLine(self.last_tag, stripline)

class Notice(Record):
    def __init__(self, config, lines, recordtype, fieldsdefinition):
        Record.__init__(self, config, lines, recordtype, fieldsdefinition)
        self.filter()

    def _walkLines(self, lines):
        tag_length = self.config['isi']['tag_length']
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
                i+=1
                continue
            if tag not in self.fields.keys() and tag != multiline:
                if 'subfields' in self.config['isi'][self.recordtype] and tag in subfields.keys():
                    i = self.appendSubfield(tag, i, lines)
                    continue
                else:
                    i+=1
                    continue
            self.dispatchValidLine(tag, line)
            i+=1

    def appendSubfield(self, tag, i, lines):
        sublines=[]
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
                i+=1
                return i
            else:
                sublines += [subline]
            i+=1
        return i

    def clean(self):
        self.__delattr__('total_lines')
        self.__delattr__('config')
        self.__delattr__('last_tag')
        self.__delattr__('recordtype')
        self.__delattr__('fields')

    def filter(self):
        """
        apply configured filters
        TODO : filter names call methods
        summary : search for an expression into the required fields of notice
        """
        if 'required_fields' not in self.config['filters']: return 1
        required_fields = self.config['filters']['required_fields']
        for tag in required_fields:
            if tag not in self.__dict__:
                raise NoticeRejected("notice incomplete")
                return 0

        if 'match' not in  self.config['filters']: return 1
        match_regexp = self.config['filters']['match']['regexp']
        extraction_fields = self.config['filters']['match']['fields']
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

    def _walkLines(self, sublines):
        tag_length = self.config['isi']['tag_length']
        multiline = self.config['isi']['multiline']
        for line in sublines:
            tag = self.getTag(line)
            if tag not in self.fields.keys() and tag != multiline:
                continue
            self.dispatchValidLine(tag, line)


def main(file_isi, config, output_file, mongodb, limit=None):
    """
    Parses, filters, and save
    """
    issue_begin = re.compile(config['isi']['issues']['begin']+"\s.*$")
    issue_end = re.compile(config['isi']['issues']['end']+"\s.*$")
    begin_tag = re.compile(config['isi']['items']['begin']+"\s.*$")
    end_tag = re.compile(config['isi']['items']['end']+"\s.*$")

    if 'match' in config['filters']:
        config['filters']['match']['regexp'] = re.compile(['filters']['match']['regexp'])

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
                notice_to_file(output_file, issue_lines)
                mongodb.issues.update({"_id":issue.__dict__['_id']}, issue.__dict__, upsert=True)

            in_notice = 1
            file_lines = [line]
            continue

        if end_tag.match(line) is not None and in_notice == 1:
            in_notice = 0
            file_lines += [line]
            try:
                notice = Notice(config, file_lines, 'items', config['isi']['items']['fields'])
                total_imported += 1
                notice_to_file(output_file, file_lines)
                notice.__dict__['issue'] = issue.__dict__
                mongodb.notices.update({"_id":notice.__dict__['_id']}, notice.__dict__, upsert=True)

            except NoticeRejected, nr:
                logging.debug("%s"%nr)

            if limit is not None and total_imported >= limit:
                 return total_imported

        if issue_begin.match(line) is not None:
            in_issue = 1
            issue_lines = [line]
            continue

        if in_issue == 1:
            issue_lines += [line]
            continue
    # closes the issue item
    notice_to_file(output_file, ["RE\n"])
    return total_imported
