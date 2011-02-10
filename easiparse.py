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

from easiparse import importer
import yaml
from glob import glob
import gzip
import re
import codecs

if __name__ == "__main__":
   config = yaml.load( open( "config.yaml", 'rU' ) )
   data_path = glob(config['data_path'])

   total=0
   number_files=0
   print config

   for filepath in data_path:
      if re.match(r".+\.gz", filepath, re.I) is not None:
         try:
            isi_file = gzip.open(filepath,'rb')
         except Exception, exc:
            print "BAD GZIP at %s"%filepath
            continue
         #isi_file = open(filepath,'rU')
      else:
         continue

      subtotal = importer.main(
         isi_file,
         config,
         limit=None,
          overwrite=True
      )
      total += subtotal
      number_files += 1
      print("extracted %d matching notices in %s (done %d files, %d total notices)"%(subtotal, isi_file, number_files, total))
   print("TOTAL = %d indexed notices within the path"%total)
