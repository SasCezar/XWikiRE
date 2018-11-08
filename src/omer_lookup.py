import copy
import csv
import json

from pymongo import MongoClient

import config


def get_entities(path):
    entities = set()
    with open(path, "rt", encoding="utf8") as inf:
        for line in inf:
            row = line.split("\t")
            entity = row[2]
            entities.add(entity)

            if len(row) == 5:
                entities.add(row[4])

    print("Loaded entities = {}".format(len(entities)))
    return entities


def grouper(sequence, chunk_size):
    for j in range(0, len(sequence), chunk_size):
        chunck = sequence[j:j + chunk_size]
        yield chunck


def get_raw_property(path):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]

    entities = get_entities("C:\\Users\\sasce\\Downloads\\all_splits.csv")

    omer_props = set()
    with open("C:\\Users\sasce\PycharmProjects\WikiReading\src\\resources\omer_prop_id.txt", "rt",
              encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for pid, _ in reader:
            omer_props.add(pid)

    i = 0
    with open("entities_properties_omer.json", "wt", encoding="utf8") as outf:
        for doc in wikidata.find({}, {"_id": 0, "aliases": 0, "sitelinks": 0, "descriptions": 0}):
            try:
                if 'en' not in doc['labels'] and doc['labels']['en']['value'] not in entities:
                    continue

                i += 1
                document = copy.deepcopy(doc)
                for pid in doc['claims']:
                    if pid not in omer_props:
                        del document['claims'][pid]
                        continue
                json_doc = json.dumps(document, ensure_ascii=False)
                outf.write(json_doc + "\n")
                print(i)
            except KeyError:
                continue


get_raw_property("")
