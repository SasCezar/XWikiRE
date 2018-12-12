import copy
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


def get_entities_id(languages):
    languages_entities = {lang: set() for lang in languages}
    languages_count = defaultdict(Counter)
    for language in languages:
        logging.info("Loading {}".format(language))
        entities, ecount = get_entity_ids("{}_qa_positive.json".format(language))
        languages_count[language].update(ecount)
        # nentities, necount = get_entity_ids("{}_qa_negative.json".format(language))
        # languages_count[language].update(necount)
        # entities.update(nentities)
        logging.info("Size of '{}' is {}".format(language, len(entities)))
        languages_entities[language] = entities
    logging.info("All languages loaded")
    intersection = copy.deepcopy(languages_entities[languages[0]])
    for language in languages[1:]:
        intersection.intersection_update(languages_entities[language])
    logging.info("Size of intersection {}".format(len(intersection)))
    return intersection, languages_entities


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))

    langs = ['it', 'en', 'de', 'fr', 'es']
    intersection, _ = get_entities_id(langs)

    with open("negative_entity_intersection.txt", "wt", encoding="utf8") as outf:
        for entity in intersection:
            outf.write(entity + "\n")

    file_base = "/image/nlp-letre/QA/data/entity_split_parallel/"
    """
    for lang in langs:
        for set_type in ['dev', 'test', 'train']:
            path = file_base.format(lang=lang, type=set_type)
            lang_files = get_entities_id(path)
    """
