import copy
import csv
import json
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
        entities = get_qa_ids("{}_qa_positive.json".format(language))
        nentities = get_qa_ids("{}_qa_negative.json".format(language))
        entities.update(nentities)
        print("Size of '{}' is {}".format(language, len(entities)))
        languages_qas[language] = entities

    intersection = copy.deepcopy(languages_qas[languages[0]])
    for language in languages[1:]:
        intersection.intersection_update(languages_qas[language])

    print("Size of QAs intersection is {}".format(len(intersection)))
    return intersection, languages_qas


def random_sample_QAs(pool, size, seen_entities=None, balance=True):
    if seen_entities is None:
        seen_entities = set()
    example_count = {1: 0, 0: 0}
    examples = defaultdict(set)
    entities = set()
    for entity_id, question_id, example_type in pool:
        if entity_id not in entities and entity_id not in seen_entities and (
                size < 0 or example_count[example_type] < size / 2):
            example_count[example_type] += 1
            examples[example_type].add((entity_id, question_id, example_type))
            entities.add(entity_id)

            if len(entities) == size:
                qas = copy.deepcopy(examples[0])
                qas.update(examples[1])
                assert len(qas) == len(entities)
                return entities, qas

    if balance and False:
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
    assert len(qas) == len(entities)
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
        print("Size of '{}' is {}".format(language, len(entities)))
        languages_entities[language] = entities
    print("All languages loaded")
    intersection = copy.deepcopy(languages_entities[languages[0]])
    for language in languages[1:]:
        intersection.intersection_update(languages_entities[language])
    print("Size of intersection {}".format(len(intersection)))
    return intersection, languages_entities


def split_entity():
    languages = ['it', 'en']
    qas_intersection, language_qas = get_qa_intersection(languages)

    # entity_intersection, language_entities = get_intersection(languages)

    pool = set(copy.deepcopy(qas_intersection))
    used = set()
    used_all = set()
    examples = defaultdict(set)
    for set_name, count in [('test', 10000), ('dev', 2000), ('train', -1)]:
        print("Starting: {}".format(set_name))
        used_entities, set_ids = random_sample_QAs(pool, count, used_all)
        examples[set_name].add(set_ids)
        if set_name != 'train':
            used.update(used_entities)
        used_all.update(used_entities)
        write_set_ids(set_ids, "parallel_{}_{}_set.txt".format("-".join(languages), set_name))
        print("Processed: {}".format(set_name))

    print("Used: {} entities".format(len(used)))
    print("Used all: {} entities".format(len(used_all)))

    examples = defaultdict(set)
    for language in languages:
        lang_pool = {x for x in language_qas[language] if x[0] not in used}
        print("Language {} pool size = {}".format(language, len(lang_pool)))
        used_entities, set_ids = random_sample_QAs(lang_pool, 1000000, used)
        write_set_ids(set_ids, "{}_{}_set.txt".format(language, "train"))
        examples[language].add(set_ids)


split_entity()


def check_duplicates():
    seen = set()
    test_seen = set()
    for set_typ in ['test', 'train', 'dev']:

        with open("parallel_it-en_{}_set.txt".format(set_typ), "rt", encoding="utf8") as inf:
            reader = csv.reader(inf, delimiter="\t")
            for eid, _, _ in reader:
                if eid in seen:
                    print(set_typ)
                else:
                    seen.add(eid)
                    if set_typ != 'train':
                        test_seen.add(eid)

    for lang in ['it', 'en']:
        cpy_seen = copy.deepcopy(test_seen)
        with open("{}_train_set.txt".format(lang), "rt", encoding="utf8") as inf:
            reader = csv.reader(inf, delimiter="\t")
            for eid, _, _ in reader:
                if eid in cpy_seen:
                    print("Error {}".format(lang))
                else:
                    cpy_seen.add(eid)

    print(len(seen))


check_duplicates()


def extract_datasets():
    sets = ['test', 'dev', 'train']
    files = {}
    p_test = "parallel_it-en_test_set.txt"
    p_def = "parallel_it-en_test_set.txt"
    p_test = "parallel_it-en_test_set.txt"
