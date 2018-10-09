from pymongo import MongoClient

import config

MAX_DOCS_FILE = 1000


def export_for_pos():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikimerge = db[config.WIKIMERGE_COLLECTION]

    cursor = wikimerge.find({}, {"id": 1, "pos_tagger_sequence": 1, "_id": 0})

    i = 0
    for doc in cursor:
        file = open("{}_pos_{}.txt".format(config.LANG, i))
        j = 0
        while j < MAX_DOCS_FILE:
            file.write("<doc id={}\n\n".format(doc['id']))
            tokens = "\n".join(doc['pos_tagger_sequence'])
            file.write(tokens)
            file.write("\n</doc>\n\n")
            j += 1

        file.flush()
        file.close()


def import_pos():
    pass
