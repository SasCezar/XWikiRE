import json
import logging
import random
from collections import defaultdict

import mmap

import ftfy
from tqdm import tqdm


def get_num_lines(file_path):
    fp = open(file_path, "r+")
    buf = mmap.mmap(fp.fileno(), 0)
    lines = 0
    while buf.readline():
        lines += 1
    return lines


def balance(file, limit, out, keep_all):
    logging.info("Reading {}".format(file))
    examples = {"POS": defaultdict(list), "NEG": defaultdict(list)}
    with open(file, "rt", encoding="utf8") as inf:
        for line in tqdm(inf, total=get_num_lines(file)):
            obj = json.loads(line)

            if not obj['answers']:
                continue

            ex_type = "NEG" if [0, 0] in obj['answers'] else "POS"
            prop = obj['prop_id']

            examples[ex_type][prop].append(line)

    logging.info("Read of {} completed".format(file))

    logging.info("Shuffling...")
    for ex_type in ['POS', 'NEG']:
        for p in examples[ex_type]:
            random.shuffle(examples[ex_type][p])

    logging.info("Shuffling completed")

    logging.info("Writing")
    with open(out, "wt", encoding="utf8") as outf:
        for ex_type in ['NEG', 'POS']:
            count = 0
            skipped = 0
            seen = set()
            while count < limit and skipped < len(examples[ex_type]):
                for p in examples[ex_type]:
                    if examples[ex_type][p]:
                        obj = json.loads(examples[ex_type][p].pop())
                        if not keep_all:
                            key = obj['q_id']
                            if key not in seen:
                                seen.add(key)
                            else:
                                continue

                        row = json.dumps(obj, ensure_ascii=False)
                        outf.write(row + "\n")
                        skipped = 0
                        count += 1
                    else:
                        skipped += 1

                    if count == limit:
                        break

#TODO Add arguments
if __name__ == '__main__':
    base_path = "/image/nlp-letre/QA/data/relation_split_5/{lang}/{fold}/{prepend}examples_{set}_{fold}.json"
    for lang in ['en']:
        for fold in [0]:  # , 1, 2, 3, 4]:
            # for type_set, limit, keep in [('dev', 2500, False), ('train', 500000, True), ('test', 5000, False)]:
            for type_set, limit, keep in [('dev', 2500, False), ('test', 5000, False)]:
                infile = base_path.format(lang=lang, fold=fold, set=type_set, prepend='')
                outfile = base_path.format(lang=lang, fold=fold, set=type_set, prepend='balanced_')
                balance(infile, limit, outfile, keep)
