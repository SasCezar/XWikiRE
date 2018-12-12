import csv
import hashlib
import json
import logging
import sys

from pymongo import MongoClient

import config


def load_props(path="/resources/levy_et_al_properties.txt"):
    omer_props = set()
    with open(path, "rt", encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for pid, _ in reader:
            omer_props.add(pid)

    return omer_props


def get_id_for_qa(page_id, prop_id, answer_id):
    unique_str = " ".join([page_id, prop_id, answer_id])
    return hashlib.sha1(unique_str.encode("utf-8")).hexdigest()


def export(out_path):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wiki_srl = db[config.WIKIMERGE_COLLECTION]
    documents = wiki_srl.find({}, {"_id": 0, "text": 0, "string_sequence": 0, "break_levels": 0,
                                   "pos_tagger_sequence": 0, "sentence_breaks": 0, "paragraph_breaks": 0,
                                   "label_sequence": 0})
    with open(out_path, "wt", encoding="utf8", newline="") as outf:
        for document in documents:
            us_doc = {"entity_id": document['id'], "entity_label": document['label'],
                      "entity_aliases": document['aliases']}

            omer_props = load_props("")
            for prop_id in document['facts']:
                if prop_id not in omer_props:
                    continue

                prop_label = document['properties'][prop_id]['label']
                prop_aliases = document['properties'][prop_id]['aliases']

                us_doc['property_id'] = prop_id
                us_doc['property_label'] = prop_label
                us_doc['property_aliases'] = prop_aliases
                us_doc['property_aliases'] = prop_aliases
                for fact in document['facts'][prop_id]:
                    value_id = fact['id']
                    value_type = fact['type']
                    value_label = fact['label'] if 'label' in fact else fact['value']
                    value_aliases = fact.get('aliases', [])

                    us_doc['value_id'] = value_id
                    us_doc['value_type'] = value_type
                    us_doc['value_label'] = value_label
                    us_doc['value_aliases'] = value_aliases

                    us_doc['id'] = get_id_for_qa(document['id'], prop_id, value_id)

                    us_string = json.dumps(us_doc, ensure_ascii=False)

                    outf.write(us_string + "\n")


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))
    # build_us({})
    export("{}_universal_schema.json".format(config.LANG))
    logging.info("Completed %s", " ".join(sys.argv))
