# -*- coding: utf-8 -*-
import os, sys
import re

import logging
logging.basicConfig(level=logging.WARNING, format="%(levelname)-8s %(message)s")
import os, sys
import codecs
import exceptions

from pymongo import Connection
MONGODB_PORT = 27017


def notice_to_file(output_file, notice_lines):
   """
   Copies notice lines to output file
   """
   for line in notice_lines:
      output_file.write( line )
   logging.debug("written %d lines to %s"%(len(notice_lines),output_file))


class NoticeRejected(exceptions.Exception):
   pass


class Notice(object):
   def __init__(self, config, lines):

      self.config = config
      tag_length = config['isi']['tag_length']
      multiline = config['isi']['multiline']
      fields = config['isi']['fields']
      subfields = config['isi']['subfields']
      i = 0
      self.total_lines = len(lines)
      while 1:
         if i == self.total_lines: break
         line = lines[i]
         tag = self.getTag(line)
         if tag == "UT":
            self.__dict__["_id"] = self.getLine(line)
            i+=1
            continue
         if tag not in fields.keys() and tag != multiline:
            if tag in subfields.keys():
               i = self.appendSubfield(tag, i, lines)
               continue
            else:
               i+=1
               continue

         self.dispatchValidLine(tag, line)
         i+=1

      self.normalize(fields)
      self.filter()


   def appendSubfield(self, tag, i, lines):
      sublines=[]
      i += 1
      while 1:
         if i == self.total_lines:
            raise NoticeRejected("reached EOF without closing a Notice")
         subline = lines[i]
         subtag = self.getTag(subline)
         if subtag == self.config['isi']['subfields'][tag]['end']:
            subfield = SubRecord(self.config, tag, sublines).__dict__
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

   def filter(self):
      """
      search for an expression into the required fields of notice
      """
      match_regexp = self.config['match_regexp']
      required_fields = self.config['required_fields']
      extraction_fields = self.config['extraction_fields']
      self.__delattr__('total_lines')
      self.__delattr__('last_tag')
      self.__delattr__('config')
      for tag in required_fields:
         if tag not in self.__dict__:
            raise NoticeRejected("notice incomplete")
            return 0

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

   def normalize(self, field_rules):
      for tag, rule in field_rules.iteritems():
         if tag not in self.__dict__.keys(): continue
         if type(rule) == str:
            self.__dict__[tag] = rule.join(self.__dict__[tag])

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

class SubRecord(Notice):
   def __init__(self, config, tag, sublines):
      self.config = config
      tag_length = config['isi']['tag_length']
      multiline = config['isi']['multiline']
      fields = config['isi']['subfields'][tag]['fields']
      for line in sublines:
         tag = self.getTag(line)
         if tag not in fields.keys() and tag != multiline:
            continue
         self.dispatchValidLine(tag, line)
      self.normalize(fields)
      self.filter()

   def filter(self):
      """
      clean unwanted attributes
      """
      self.__delattr__('config')
      self.__delattr__('last_tag')

def main(file_isi, config, limit=None, overwrite=False):
   """
   Parses, filters, and save
   """
   output_file = codecs.open( config['output_file'], "w+", encoding="utf_8", errors='replace' )
   mongodb = Connection("localhost", MONGODB_PORT)[config['bdd_name']]

   if overwrite is True and "notices" in mongodb.collection_names():
      mongodb.drop_collection("notices")

   begin_tag = re.compile(config['isi']['begin']+"\s.*$")
   end_tag = re.compile(config['isi']['end']+"\s.*$")
   config['match_regexp'] = re.compile(config['match_regexp'])
   file_lines = []
   in_notice = 0
   total_imported = 0

   # itere sur les lignes du corpus
   for line in file_isi:
      if in_notice == 1 and end_tag.match(line) is None:
         file_lines += [line]
      elif begin_tag.match(line) is not None and in_notice == 0:
         in_notice = 1
         file_lines = [line]
      elif end_tag.match(line) is not None and in_notice == 1:
         in_notice = 0
         file_lines += [line]
         try:
            notice = Notice(config, file_lines)
            total_imported += 1
            notice_to_file(output_file, file_lines)
            mongodb.notices.save(notice.__dict__)
         except NoticeRejected, nr:
            logging.debug("%s"%nr)
            pass
         if limit is not None and total_imported >= limit:
            return total_imported
      else:
         #logging.debug("line skipped : not between notice TAGS")
         continue

   return total_imported
