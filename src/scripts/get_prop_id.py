import csv

from pymongo import MongoClient

import config

client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
db = client[config.DB]
wikidata = db[config.WIKIDATA_COLLECTION]

prop_labels = []
with open("../resources/omer_prop.txt", "rt", encoding="utf8") as inf:
    for line in inf:
        prop_labels.append(line.strip())

documents = list(wikidata.find({"id": {"$regex": "^P"}}, {"id": 1, "labels": 1, "aliases": 1, "_id": 0}))

seen = set()

with open("../resources/omer_prop_id.txt", "wt", encoding="utf8", newline="") as outf:
    writer = csv.writer(outf, delimiter="\t")
    processed = []
    for document in documents:
        try:
            label = document['labels']['en']['value']
            prop_id = document['id']
            if label in prop_labels:
                writer.writerow([prop_id, label])
                processed.append(label)
                seen.add(prop_id)
                continue
        except KeyError:
            continue

    diffs = set(prop_labels) - set(processed)
    for diff in diffs:
        print(diff)

    print("----------------------")

    for document in documents:
        try:
            aliases = document['aliases']['en']
            prop_id = document['id']
            for alias in aliases:
                label = alias['value']
                if label in diffs and prop_id not in seen:
                    writer.writerow([prop_id, label])
                    processed.append(label)
                    seen.add(prop_id)
                    continue
        except KeyError:
            continue

    diffs = set(prop_labels) - set(processed)
    for diff in diffs:
        print(diff)
