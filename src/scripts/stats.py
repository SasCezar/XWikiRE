import copy
import csv
import itertools
from collections import defaultdict

from pymongo import MongoClient

import config


def get_prop_count(langs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    for lang in langs:
        print("Processing lang: {}".format(lang))
        wikipedia = db["{}wiki_omer".format(lang)]

        omer_props = get_omer_props()

        total = 0
        for ex_type in ['positive', 'negative']:
            print("Computing: {}".format(ex_type))
            prop_count = {key: 0 for key in omer_props}
            documents = wikipedia.find({}, {"QA": 1, "_id": 0})
            for doc in documents:
                qa = doc['QA']
                for prop in qa:
                    if prop not in omer_props:
                        continue
                    total += len(qa[prop])
                    for ex in qa[prop]:
                        ex_t = ex['example']
                        if ex_t == ex_type:
                            prop_count[prop] += 1

            with open("{}_{}_example_stats.tsv".format(lang, ex_type), "wt", encoding="utf8", newline="") as out_pos:
                writer = csv.writer(out_pos, delimiter="\t")
                writer.writerow(["Prop", "Count"])
                prop_count_it = prop_count.items()
                prop_count_it = sorted(prop_count_it, key=lambda x: -x[1])
                for prop, count in prop_count_it:
                    writer.writerow([prop, count])


def get_omer_props(path="C:\\Users\sasce\PycharmProjects\WikiReading\src\\resources\omer_prop_id.txt"):
    omer_props = set()
    with open(path, "rt",
              encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for pid, _ in reader:
            omer_props.add(pid)
    return omer_props


def get_combinations(iterable):
    result = []
    for length in range(2, len(iterable) + 1):
        result.extend(itertools.combinations(iterable, length))

    return result


def get_qa_id_itersection(langs, etype='positive'):
    omer_props = get_omer_props()

    qa_ids_per_prop_and_lang = {lang: {key: set() for key in omer_props} for lang in langs}
    for lang in langs:
        print("Processing lang {}".format(lang))
        client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
        db = client[config.DB]
        wikipedia = db["{}wiki_omer".format(lang)]
        documents = wikipedia.find({}, {"QA": 1, "_id": 0})

        for doc in documents:
            qa = doc['QA']
            for prop in qa:
                if prop not in omer_props:
                    continue
                for ex in qa[prop]:
                    ex_type = ex['example']
                    if ex_type == etype:
                        qa_ids_per_prop_and_lang[lang][prop].add(ex['id'])

    qa_ids_per_prop_combs = defaultdict(lambda: {key: 0 for key in omer_props})
    for comb in get_combinations(langs):
        for prop in omer_props:
            elements = copy.deepcopy(qa_ids_per_prop_and_lang[comb[0]][prop])
            for lang in comb[1:]:
                elements.intersection_update(qa_ids_per_prop_and_lang[lang][prop])

            qa_ids_per_prop_combs[comb][prop] = len(elements)

    for key in qa_ids_per_prop_combs:
        with open("{}_qa_intersection_{}.txt".format("-".join(key), etype), "wt", encoding="utf8", newline="") as outf:
            writer = csv.writer(outf)
            for prop in qa_ids_per_prop_combs[key]:
                writer.writerow([prop, qa_ids_per_prop_combs[key][prop]])


def get_props(langs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    for lang in langs:
        print("Processing lang: {}".format(lang))
        wikipedia = db["{}wiki_omer".format(lang)]

        omer_props = get_omer_props()

        total = 0
        documents = wikipedia.find({}, {"QA": 1, "_id": 0})
        with open("{}_qas_ids.tsv".format(lang), "wt", encoding="utf8", newline="") as out_pos:
            writer = csv.writer(out_pos, delimiter="\t")
            writer.writerow(["Prop", "Count"])

            for doc in documents:
                qa = doc['QA']
                for prop in qa:
                    if prop not in omer_props:
                        continue
                    total += len(qa[prop])
                    for ex in qa[prop]:
                        ex_id = ex['id']
                        ex_type = 1 if ex['example'] == 'positive' else 0
                        writer.writerow([prop, ex_id, ex_type])


LANGS = ['it', 'es', 'fr', 'en', 'de', 'kn']
get_props(LANGS)
get_prop_count(LANGS)
get_qa_id_itersection(LANGS, 'positive')
get_qa_id_itersection(LANGS, 'negative')
