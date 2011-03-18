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

from optparse import OptionParser
import yaml

from easiparse import importer, extractor, cooccurrences

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")


def get_parser():
    parser = OptionParser()
    parser.add_option("-e", "--execute", dest="execute", help="execution action")
    return parser

if __name__ == "__main__":
    parser = get_parser()
    (options, args) = parser.parse_args()
    print options, args
    config = yaml.load( open( "config.yaml", 'rU' ) )

    if options.execute=='import':
        importer.main_multiprocessing(config)

    if options.execute=='extract':
        extractor.main(config)

    if options.execute=='cooccurrences':
        cooccurrences.main_cooccurrences(config)

    if options.execute=='occurrences':
        cooccurrences.main_occurrences(config)