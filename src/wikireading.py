import argparse
import logging

from create_wikireading import build_vocabs, make_wikireading
from merge_wikis import wikimerge
from pos_tagger import export_for_pos

MERGE = "merge"
BUILD = "build"
EXTRACT = "extract"
VOCABS = "vocabs"

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description="Builds a dictionary mapping between Wikipedia IDs and Wikidata IDs")
    subparser = parser.add_subparsers(dest="subparser")
    merge = subparser.add_parser(MERGE)
    build = subparser.add_parser(BUILD)
    vocabs = subparser.add_parser(VOCABS)

    args = parser.parse_args()

    if args.subparser == MERGE:
        wikimerge()

    if args.subparser == EXTRACT:
        export_for_pos()

    if args.subparser == VOCABS:
        build_vocabs()

    if args.subparser == BUILD:
        make_wikireading()

