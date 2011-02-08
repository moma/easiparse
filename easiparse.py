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
    parameters_user = yaml.safe_load( file( "config.yaml", 'rU' ) )
    
    bdd_name =  parameters_user['bdd_name']
    data_path = glob(parameters_user['data_path'])
    param_corpus_name = parameters_user['isi_spec']
    match_regexp = parameters_user['match_regexp']

    dico_tag = importer.lire_parametre_ini(param_corpus_name)
    total=0
    
    print data_path
    
    for filepath in data_path:
        if re.match(r".+\.gz", filepath, re.I) is not None:
            isi_file = gzip.open(filepath,'rb')
        else:
            continue
        print "%s"%isi_file
        subtotal = importer.main(
            isi_file, 
            bdd_name, 
            dico_tag, 
            match_regexp, 
            limit=100, 
            overwrite=True
        )
        total += subtotal
    print("TOTAL = %d indexed notices"%total)
