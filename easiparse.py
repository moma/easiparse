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

if __name__ == "__main__":
   config = yaml.safe_load( file( "config.yaml", 'rU' ) )
   data_path = glob(config['config']['data_path'])
   total=0
   print config

   for filepath in data_path:
      if re.match(r".+\.gz", filepath, re.I) is not None:
         isi_file = gzip.open(filepath,'rb')
         #isi_file = open(filepath,'rU')
      else:
         continue

      subtotal = importer.main(
         isi_file,
         config,
         limit=100,
         overwrite=True
      )
      total += subtotal
      print("extracted %d matching notices in %s"%(subtotal,isi_file))
   print("TOTAL = %d indexed notices within the path"%total)
