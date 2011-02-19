
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
import exceptions

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")

def getConfiguredFilters(config):

    filters = []
    for name in config['filters'].iterkeys():
        try:
            if name == "required_fields":
                filters.append(RequiredFields(config))
            if name == "regexp_content":
                filters.append(RegexpFilter(config))
        except FilterNotDefined, fnd:
            logging.warning("filter named %s configured but not recognized"%name)
            continue
    return filters

class FilterNotDefined(exceptions.Exception):
    pass

class NoticeRejected(exceptions.Exception):
    pass

class NoticeFilter(object):
    def __init__(self, config):
        self.config = config
        if 'filters' not in self.config or self.config['filters'] is None:
            raise FilterNotDefined("no filters at all defined in %s"%self.config)
            return

    def getRules(self):
        if self.name not in self.config['filters']:
            raise FilterNotDefined("filter named %s not defined in %s"%(self.name,self.config))
            return
        return self.config['filters'][self.name]
    
    def apply(self, record):
        """
        apply an empty filter
        """
        return 1


class RequiredFields(NoticeFilter):

    name = "required_fields"

    def apply(self, record):
        """
        filters notices without user defined minimal set of fields
        """
        required_fields = self.getRules()
        for tag in required_fields:
            if tag not in record:
                raise NoticeRejected("notice incomplete")
                return 0

class RegexpFilter(NoticeFilter):
    
    name = "regexp_content"

    def apply(self, record):
        """
        filters notices not match a regular expression
        """
        rules = self.getRules()
        match_regexp = re.compile( rules['regexp'] )
        extraction_fields = rules['fields']

        for tag in extraction_fields:
            if tag not in record: continue
            if type(record[tag]) == str or type(record[tag]) == unicode:
                if match_regexp.search(record[tag]) is not None:
                    return 1
            elif type(record[tag]) == list:
                for field in record[tag]:
                    if match_regexp.search(field) is not None:
                        return 1
        # anyway : reject
        raise NoticeRejected("notice did not match the regexp")
        return 0