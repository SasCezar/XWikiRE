import csv

from pymongo import MongoClient

import config


def get_prop_count():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.OMERMERGE_COLLECTION]
    documents = wikipedia.find({}, {"QA": 1, "_id": 0})

    omer_props = set()
    with open("C:\\Users\sasce\PycharmProjects\WikiReading\src\\resources\omer_prop_id.txt", "rt",
              encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for pid, _ in reader:
            omer_props.add(pid)

    total = 0
    pos_prop_count = {key: 0 for key in omer_props}
    neg_prop_count = {key: 0 for key in omer_props}
    for doc in documents:
        qa = doc['QA']
        for prop in qa:
            if prop not in omer_props:
                continue
            total += len(qa[prop])
            for ex in qa[prop]:
                ex_type = ex['example']
                if ex_type == "positive":
                    pos_prop_count[prop] += 1
                else:
                    neg_prop_count[prop] += 1

    with open("pos_example_stats.tsv", "wt", encoding="utf8", newline="") as out_pos:
        writer = csv.writer(out_pos, delimiter="\t")
        writer.writerow(["Prop", "Count"])
        prop_count = pos_prop_count.items()
        prop_count = sorted(prop_count, key=lambda x: -x[1])
        for prop, count in prop_count:
            writer.writerow([prop, count])

    with open("neg_example_stats.tsv", "wt", encoding="utf8", newline="") as out_neg:
        writer = csv.writer(out_neg, delimiter="\t")
        writer.writerow(["Prop", "Count"])
        prop_count = neg_prop_count.items()
        prop_count = sorted(prop_count, key=lambda x: -x[1])
        for prop, count in prop_count:
            writer.writerow([prop, count])


get_prop_count()
