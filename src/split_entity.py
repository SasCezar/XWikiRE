import argparse
import copy
import csv
import json
import logging
import sys
from collections import defaultdict, Counter


def get_entity_ids(file):
    with open(file, "rt", encoding="utf8") as inf:
        ids = set()
        count = Counter()
        for line in inf:
            qa = json.loads(line)
            id = qa['entity_id']
            ids.add(id)
            count.update(id)

    return ids, count


def get_qa_ids(file):
    with open(file, "rt", encoding="utf8") as inf:
        ids = set()
        for line in inf:
            qa = json.loads(line)
            entity_id = qa['entity_id']
            qa_id = qa['id']
            qa_type = qa['na']
            ids.add((entity_id, qa_id, qa_type))

    return ids


def get_qa_intersection(languages):
    languages_qas = {lang: set() for lang in languages}
    for language in languages:
        logging.info("Loading '{}'".format(language))
        entities = get_qa_ids("{}_qa_positive.json".format(language))
        logging.info("Loaded {} positive".format(len(entities)))
        nentities = get_qa_ids("{}_qa_negative.json".format(language))
        logging.info("Loaded {} negative".format(len(nentities)))
        entities.update(nentities)
        logging.info("Size of '{}' is {}".format(language, len(entities)))
        languages_qas[language] = entities

    intersection = copy.deepcopy(languages_qas[languages[0]])
    for language in languages[1:]:
        intersection.intersection_update(languages_qas[language])

    logging.info("Size of QAs intersection is {}".format(len(intersection)))
    return intersection, languages_qas


def random_sample_qas(pool, size, seen_entities=None, balance=True, keep_all=False):
    if seen_entities is None:
        seen_entities = set()
    example_count = {1: 0, 0: 0}
    examples = defaultdict(set)
    entities = set()
    for entity_id, question_id, example_type in pool:
        if entity_id not in seen_entities and (keep_all or (entity_id not in entities and (
                size < 0 or example_count[example_type] < size / 2))):
            example_count[example_type] += 1
            examples[example_type].add((entity_id, question_id, example_type))
            entities.add(entity_id)

            if len(entities) == size:
                qas = copy.deepcopy(examples[0])
                qas.update(examples[1])
                return entities, qas

    if balance:
        sizes = [example_count[0], example_count[1]]
        min_size = min(sizes)
        min_item = sizes.index(min_size)
        max_set = list(examples[(1 - 1 * min_item)])
        new_set = set(max_set[:min_size])
        examples[(1 - 1 * min_item)] = new_set
        unused_entities = {entity_id for entity_id, _, _ in max_set[min_size:]}
        entities.difference_update(unused_entities)

    qas = copy.deepcopy(examples[0])
    qas.update(examples[1])
    return entities, qas


def write_set_ids(items, file):
    with open(file, "wt", encoding="utf8", newline='') as outf:
        writer = csv.writer(outf, delimiter="\t")
        for item in items:
            writer.writerow(list(item))


def get_intersection(languages):
    languages_entities = {lang: set() for lang in languages}
    languages_count = defaultdict(Counter)
    for language in languages:
        entities, ecount = get_entity_ids("{}_qa_positive.json".format(language))
        languages_count[language].update(ecount)
        nentities, necount = get_entity_ids("{}_qa_negative.json".format(language))
        languages_count[language].update(necount)
        entities.update(nentities)
        logging.info("Size of '{}' is {}".format(language, len(entities)))
        languages_entities[language] = entities
    logging.info("All languages loaded")
    intersection = copy.deepcopy(languages_entities[languages[0]])
    for language in languages[1:]:
        intersection.intersection_update(languages_entities[language])
    logging.info("Size of intersection {}".format(len(intersection)))
    return intersection, languages_entities


def split_entity(languages):
    qas_intersection, language_qas = get_qa_intersection(languages)

    # entity_intersection, language_entities = get_intersection(languages)

    pool = set(copy.deepcopy(qas_intersection))
    used = set()
    used_all = set()
    for set_name, count, keep_all in [('test', 10000, False), ('dev', 2000, False), ('train', 1000000, True)]:
        logging.info("Starting: {}".format(set_name))
        used_entities, set_ids = random_sample_qas(pool, count, used_all, keep_all=keep_all)
        if set_name != 'train':
            used.update(used_entities)
        used_all.update(used_entities)
        write_set_ids(set_ids, "parallel_ids_{}_{}_set.txt".format("-".join(languages), set_name))
        logging.info("Processed: {}".format(set_name))

    logging.info("Used: {} entities".format(len(used)))
    logging.info("Used all: {} entities".format(len(used_all)))

    for language in languages:
        lang_pool = {x for x in language_qas[language] if x[0] not in used}
        logging.info("Language {} pool size = {}".format(language, len(lang_pool)))
        used_entities, set_ids = random_sample_qas(lang_pool, 1000000, used, keep_all=True)
        f = "-".join(languages)
        write_set_ids(set_ids, "ids_{}_train_set_for-{}.txt".format(language, f))


def check_duplicates():
    seen = set()
    test_seen = set()
    for set_typ in ['test', 'train', 'dev']:
        filename = "parallel_ids_it-en_{}_set.txt".format(set_typ)
        with open(filename, "rt", encoding="utf8") as inf:
            reader = csv.reader(inf, delimiter="\t")
            for eid, _, _ in reader:
                if eid in seen:
                    logging.info("Error in {} - Duplicate item id {}".format(filename, eid))
                else:
                    seen.add(eid)
                    if set_typ != 'train':
                        test_seen.add(eid)

    for lang in ['it', 'en']:
        cpy_seen = copy.deepcopy(test_seen)
        filename = "ids_{}_train_set.txt".format(lang)
        with open(filename, "rt", encoding="utf8") as inf:
            reader = csv.reader(inf, delimiter="\t")
            for eid, _, _ in reader:
                if eid in cpy_seen:
                    logging.info("Error in {} - Duplicate item id {}".format(filename, eid))
                else:
                    cpy_seen.add(eid)

    logging.info(len(seen))


def load_qas(file, ids):
    with open(file, "rt", encoding="utf8") as inf:
        qas = defaultdict(list)
        for line in inf:
            qa = json.loads(line)
            qa_id = qa['id']
            if qa_id in ids:
                qas[qa_id].append(qa)

    return qas


def read_set_qas(file):
    ids = set()
    with open(file, "rt", encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for _, qa_id, _ in reader:
            ids.add(qa_id)

    return ids


def extract_entity_split_datasets(languages):
    sets = ['test', 'dev', 'train']

    ids = set()
    logging.info("Loading parallel QAs ids")
    for set_type in sets:
        ids.update(read_set_qas("parallel_ids_{}_{}_set.txt".format("-".join(languages), set_type)))
    logging.info("Loaded parallel QAs ids")

    logging.info("Loading QAs ids")
    for language in languages:
        f = "-".join(languages)
        ids.update(read_set_qas("ids_{}_train_set_for-{}.txt".format(language, f)))

    logging.info("Loading QAs ids")

    for language in languages:
        qas = load_qas("{}_qa_positive.json".format(language), ids)
        qas.update(load_qas("{}_qa_negative.json".format(language), ids))
        for set_type in sets:
            set_qas = read_set_qas("parallel_ids_{}_{}_set.txt".format("-".join(languages), set_type))
            with open("qas_{}_parallel_{}_{}_set.json".format(language, "-".join(languages), set_type), "wt",
                      encoding="utf8") as outf:
                for qid in set_qas:
                    for template in qas[qid]:
                        string_qa = json.dumps(template, ensure_ascii=False)
                        outf.write(string_qa + "\n")

    for language in languages:
        qas = load_qas("{}_qa_positive.json".format(language), ids)
        qas.update(load_qas("{}_qa_negative.json".format(language), ids))
        logging.info("Creating {} ".format(language))
        f = "-".join(languages)
        file = "ids_{}_train_set_for-{}.txt".format(language, f)
        logging.info("IDs File = {}".format(file))
        set_qas = read_set_qas(file)
        qfile = "qas_{}_train_set_for-{}.json".format(language, f)
        with open(qfile, "wt", encoding="utf8") as outf:
            logging.info("Writing {}".format(qfile))
            for qid in set_qas:
                for template in qas[qid]:
                    string_qa = json.dumps(template, ensure_ascii=False)
                    outf.write(string_qa + "\n")

    logging.info("Created splits")


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))

    parser = argparse.ArgumentParser(description="Builds a parallel corpus on entities.")
    parser.add_argument('-l', '--langs', help='Languages used to create a parallel split', required=True, nargs='+')

    args = parser.parse_args()

    split_entity(args.langs)
    logging.info("Split complete")
    extract_entity_split_datasets(args.langs)
    logging.info("Completed %s", " ".join(sys.argv))
