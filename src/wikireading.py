import argparse
import logging
import multiprocessing

from vocabs import build_vocabs
from merge_wikis import wikimerge

MERGE = "merge"
BUILD = "build"
VOCABS = "vocabs"

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description="Builds a dictionary mapping between Wikipedia IDs and Wikidata IDs")
    parser.add_argument('--lang', help='Language code', dest="lang", required=True, type=str)
    parser.add_argument('--ext_lang', help='Extended string for language', dest="ext_lang", required=True, type=str)
    parser.add_argument('--locale', help="Locale used for date output", dest="locale", required=True, type=str)
    parser.add_argument('--workers', help="Number of workers", dest="workers", default=multiprocessing.cpu_count() - 1,
                        type=int)

    subparser = parser.add_subparsers(dest="subparser")
    merge = subparser.add_parser(MERGE)
    build = subparser.add_parser(BUILD)
    vocabs = subparser.add_parser(VOCABS)

    args = parser.parse_args()

    if args.subparser == MERGE:
        wikimerge({})

    if args.subparser == VOCABS:
        build_vocabs()

    if args.subparser == BUILD:
        make_wikireading()
