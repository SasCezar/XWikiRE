import difflib
import json
import logging
import mmap
from collections import defaultdict

import ftfy
from tqdm import tqdm


def get_num_lines(file_path):
    fp = open(file_path, "r+")
    buf = mmap.mmap(fp.fileno(), 0)
    lines = 0
    while buf.readline():
        lines += 1
    return lines


def extract(source_file, target_file, out_file):
    logging.info("Reading {}".format(source_file))
    ids = set()
    with open(source_file, "rt", encoding="utf8") as inf:
        for line in tqdm(inf, total=get_num_lines(source_file)):
            obj = json.loads(line)
            qid = obj['q_id']
            ids.add(qid)

    logging.info("Read {}".format(len(ids)))

    logging.info("Reading {}".format(target_file))
    with open(target_file, "rt", encoding="utf8") as inf, \
            open(out_file, "wt", encoding="utf8") as outf:
        for line in tqdm(inf, total=get_num_lines(target_file)):
            obj = json.loads(line)
            qid = obj['id']
            key = qid
            if key in ids:
                outf.write(line.strip() + "\n")
                ids.remove(key)

        logging.info(len(ids))
        for k in ids:
            logging.info(k)
        assert len(ids) == 0

# TODO Add arguments and logging
if __name__ == '__main__':
    base_path = "/image/nlp-letre/QA/data/relation_split_5/{lang}/{fold}/{prepend}examples_{set}_{fold}.json"
    for lang in ['en']:
        for fold in [0]:  # , 1, 2, 3, 4]:
            for type_set, limit in [('dev', 5000), ('test', 10000)]:  # , ('train', 1000000), ('test', 10000)]:
                source_file = base_path.format(lang=lang, fold=fold, set=type_set, prepend='balanced_')
                target_file = "/image/nlp-letre/QA/data/relation_split_5/{lang}/{fold}/{lang}_rel_{set}_{fold}.json" \
                    .format(lang=lang, fold=fold, set=type_set, prepend='')
                outfile = base_path.format(lang=lang, fold=fold, set=type_set, prepend='unprocessed_balanced_')

                extract(source_file, target_file, outfile)
