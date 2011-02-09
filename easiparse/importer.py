# -*- coding: utf-8 -*-
import os, sys
import re

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")
import os, sys
import codecs

from pymongo import Connection
MONGODB_PORT = 27017

def filter_notice(config, notice):
   """
   search for an expression into the required fields of notice
   """
   match_regexp = config['config']['match_regexp']
   required_fields = config['config']['required_fields']
   extraction_fields = config['config']['extraction_fields']

   for tag in required_fields:
      if tag not in notice:
         logging.debug("notice incomplete")
         return 0

   for tag in extraction_fields:
      if match_regexp.match(notice[tag]) is not None:
         logging.debug("matched %s in notice %s"%(str(match_regexp),notice))
         return 1
   return 0

def notice_to_file(output_file, notice_lines):
   """
   Copies notice lines to output file
   """
   for line in notice_lines:
      output_file.write( line + "\n" )
   logging.debug("written %d lines to %s"%(len(notice_lines),output_file))


class Notice(object):
   def __init__(self, config, lines):
      self.config = config
      tag_length = config['config']['isi']['tag_length']
      multiline = config['config']['isi']['multiline']
      fields = config['config']['isi']['fields']
      for line in lines:
         tag = self.getTag(line)
         if tag not in fields.keys() and tag != multiline:
            continue
         stripline = line[ tag_length : ].strip()
         if tag != multiline:
            last_tag = tag
            self.appendLine(tag, stripline)
         else:
            self.appendLine(last_tag, stripline)
      self.normalize()

   def normalize(self):
      for tag, rule in self.config['config']['isi']['fields'].iteritems():
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
      return line[ : self.config['config']['isi']['tag_length'] ].strip()

def main(file_isi, config, limit=None, overwrite=False):
   """
   Parses, filters, and save
   """
   output_file = codecs.open( config['config']['output_file'], "w+", encoding="utf_8", errors='replace' )
   mongodb = Connection("localhost", MONGODB_PORT)[config['config']['bdd_name']]

   if overwrite is True and "notices" in mongodb.collection_names():
      mongodb.drop_collection("notices")

   begin_tag = re.compile(config['config']['isi']['begin']+"\s.*$",re.I)
   end_tag = re.compile(config['config']['isi']['end']+"\s.*$",re.I)

   file_lines = []
   in_notice = 0
   total_imported = 0

   # itere sur les lignes du corpus
   for line in file_isi.readlines():
      if in_notice == 1 and end_tag.match(line) is None:
         file_lines += [line]
      elif begin_tag.match(line) is not None and in_notice == 0:
         logging.debug("start notice")
         in_notice = 1
         file_lines = [line]
      elif end_tag.match(line) is not None and in_notice == 1:
         logging.debug("end notice")
         in_notice = 0
         file_lines += [line]
         notice = Notice(config, file_lines)
         mongodb.notices.save(notice.__dict__)
         #if filter_notice(config, notice.__dict__) == 1:
         #   total_imported += 1
         #   notice_to_file(output_file, file_lines)
         #   mongodb.notices.save(notice.__dict__)
         del notice
         if total_imported >= limit:
            return
      else:
         logging.warning("line skipped : not between notice TAGS")
         continue

   return total_imported
