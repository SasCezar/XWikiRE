import argparse
import logging
import pickle
import re
import sys

WIKIBASE_ITEM_RE = re.compile(r"\(\d+,'wikibase_item','Q\d+',\w+\)")

logger = logging.getLogger(__name__)


def extract_wikidata_to_wikipedia_mapping(file_in, file_out):
    mapping = {}
    with open(file_in, "rt", encoding="ANSI") as inf, open(file_out, "wb") as outf:
        for line in inf:
            matches = WIKIBASE_ITEM_RE.findall(line)
            for match in matches:
                wikibase_tuple = match[1:-1].split(",")
                mapping[wikibase_tuple[0]] = wikibase_tuple[2][1:-1]
        pickle.dump(mapping, outf, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info("Completed! {} mappings were found".format(len(mapping)))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description="Builds a dictionary mapping between Wikipedia IDs and Wikidata IDs")
    parser.add_argument('-f', '--file', help='Path to Wikipedia \'page_props\' database dump (read-only).',
                        required=True)
    parser.add_argument('-o', '--out', help='Path to output file', required=True)
    args = parser.parse_args()

    logger.info("running %s", " ".join(sys.argv))
    extract_wikidata_to_wikipedia_mapping(args.file, args.out)
