import argparse
import logging
import multiprocessing

from create_wikireading import make_wikireading
from merge_wikis import merge_wikis
from pos_tagger import export_for_pos
from vocabs import build_vocabs

MERGE = "merge"
BUILD = "build"
EXTRACT = "extract"
VOCABS = "vocabs"

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description="Builds a dictionary mapping between Wikipedia IDs and Wikidata IDs")
    parser.add_argument('--lang', help='Language code', dest="lang", required=True, type=str)
    parser.add_argument('--ext_lang', help='Extended string for language', dest="ext_lang", required=True, type=str)
    parser.add_argument('--locale', help="Locale used for date output", dest="locale", required=True, type=str)
    parser.add_argument('--workers', help="Number of workers", dest="locale", default=multiprocessing.cpu_count() - 1,
                        type=int)

    subparser = parser.add_subparsers(dest="subparser")
    merge = subparser.add_parser(MERGE)
    build = subparser.add_parser(BUILD)
    vocabs = subparser.add_parser(VOCABS)

    args = parser.parse_args()

    if args.subparser == MERGE:
        merge_wikis()

    if args.subparser == EXTRACT:
        export_for_pos()

    if args.subparser == VOCABS:
        build_vocabs()

    if args.subparser == BUILD:
        make_wikireading({})
