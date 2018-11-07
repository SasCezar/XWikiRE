import copy
import csv
import itertools
from collections import defaultdict

from pymongo import MongoClient

import config


def get_prop_count():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.OMERMERGE_COLLECTION]
    documents = wikipedia.find({}, {"QA": 1, "_id": 0})

    omer_props = get_omer_props()

    total = 0
    for ex_type in ['positive', 'negative']:
        prop_count = {key: 0 for key in omer_props}
        for doc in documents:
            qa = doc['QA']
            for prop in qa:
                if prop not in omer_props:
                    continue
                total += len(qa[prop])
                for ex in qa[prop]:
                    ex_type = ex['example']
                    if ex_type == ex_type:
                        prop_count[prop] += 1

        with open("{}_{}_example_stats.tsv".format(config.LANG, ex_type), "wt", encoding="utf8", newline="") as out_pos:
            writer = csv.writer(out_pos, delimiter="\t")
            writer.writerow(["Prop", "Count"])
            prop_count = prop_count.items()
            prop_count = sorted(prop_count, key=lambda x: -x[1])
            for prop, count in prop_count:
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
    for length in range(2, len(iterable)):
        result.extend(itertools.combinations(iterable, length))

    return result


def get_qa_id_itersection():
    langs = ['it', 'es', 'fr']

    omer_props = get_omer_props()

    qa_ids_per_prop_and_lang = defaultdict(lambda _: {key: set() for key in omer_props})
    for lang in langs:
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
                    if ex_type == 'positive':
                        qa_ids_per_prop_and_lang[lang][prop].add(qa['id'])

    qa_ids_per_prop_combs = defaultdict(lambda _: {key: 0 for key in omer_props})
    for comb in get_combinations(langs):
        for prop in omer_props:
            elements = copy.deepcopy(qa_ids_per_prop_and_lang[comb[0]][prop])
            for lang in comb[1:]:
                elements.intersection_update(qa_ids_per_prop_and_lang[lang][prop])

            qa_ids_per_prop_combs[comb][prop] = len(elements)

    for key in qa_ids_per_prop_combs:
        with open("{}_qa_intersection".format("-".format(key)), "rt", encoding="utf8") as outf:
            writer = csv.writer(outf)
            for prop in qa_ids_per_prop_combs[key]:
                writer.writerow([prop, qa_ids_per_prop_combs[key][prop]])


get_qa_id_itersection()
# get_prop_count()
