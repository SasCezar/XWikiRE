import collections
import json
import logging
import multiprocessing
import re
import sys
import time
import traceback
from functools import partial
from typing import List, Dict

from natural.date import compress
from pymongo import MongoClient
from tqdm import tqdm

import config
from utils import get_chunks

STOP_SECTIONS = {
    'en': ['See also', 'Notes', 'Further reading', 'External links'],
    'fr': ['Notes et références', 'Bibliographie', 'Voir aussi', 'Annexes', 'Références'],
    'it': ['Note', 'Bibliografia', 'Voci correlate', 'Altri progetti', 'Collegamenti esterni'],
    'de': ['Literatur', 'Siehe auch', 'Weblinks', 'Anmerkungen', 'Einzelnachweise und Anmerkungen', 'Referenzen'],
    'es': ['Véase también', 'Notas', 'Referencias', 'Bibliografía', 'Enlaces externos', 'Notas y referencias']
}

STOP_SECTIONS_RE = re.compile("===?\s({})\s===?".format('|'.join(STOP_SECTIONS.get(config.LANG, []))))

NO_UNIT = {'labels': {}, 'id': '', 'aliases': {}}



def create_string_fact(value: str) -> Dict:
    value = value.strip()
    fact = {"value": value, "type": "value", "_id": value}
    return fact


def create_quantity_fact(amount: str, unit: Dict) -> Dict:
    amount = amount[1:] if amount.startswith("+") else amount
    value = amount

    labels = unit["labels"]
    aliases = unit["aliases"]
    fid = value + unit['id']

    fact = {"value": value.strip(), "type": "quantity", "_id": fid, "labels": labels, "aliases": aliases}
    if not fact["id"]:
        print("error")
    return fact


def create_time_fact(formatted_date: str, date: str):
    fact = {"value": formatted_date, "type": "date", "_id": date}
    return fact


def clean_text(text: str) -> str:
    match = STOP_SECTIONS_RE.search(text)
    if match and match.start() > 0:
        text = text[:match.start()].strip()
    text = re.sub("===?\s[^=]+\s===?\n?", "", text)
    text = re.sub("\[\d+\]", "", text)
    text = re.sub("\n{3,}", "\n\n", text)
    return text


def merge(limit, configs):
    start_time = time.time()
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]

    date_formatter = config.DATE_FORMATTER
    cache = {}
    links = []
    entity_ids = set()
    nodes = []
    dd = list(wikidata.find({"id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}))
    skipped = 0
    for wikidata_doc in dd:
        try:
            for prop_id in wikidata_doc['claims']:
                for claim in wikidata_doc['claims'][prop_id]:
                    try:
                        datatype = claim['mainsnak']['datavalue']['type']
                        if datatype == "string":
                            string_type = claim['mainsnak']['datatype']
                            if string_type in {'external-id', 'commonsMedia'}:
                                continue
                            value = claim['mainsnak']['datavalue']['value']
                            fact = create_string_fact(value)
                            nodes.append(fact)
                            links.append({"_from": wikidata_doc["id"], "type": prop_id, "_to": fact["id"]})
                        elif datatype == "wikibase-entityid":
                            d_id = claim['mainsnak']['datavalue']['value']['id']
                            entity_ids.add(d_id)
                            links.append({"_from": wikidata_doc["id"], "type": prop_id, "_to": d_id})
                        elif datatype == "quantity":
                            d_id = claim['mainsnak']['datavalue']['value']['unit'].split("/")[-1]
                            amount = claim['mainsnak']['datavalue']['value']['amount']
                            if str(d_id) != "1":
                                if d_id in cache:
                                    unit_doc = cache[d_id]
                                else:
                                    unit_doc = list(wikidata.find({"id": d_id}, {"_id": 0}))
                                    cache[d_id] = unit_doc
                            else:
                                unit_doc = []
                            if not unit_doc:
                                unit = NO_UNIT
                            else:
                                unit = unit_doc[0]
                            fact = create_quantity_fact(amount, unit)
                            if not fact["id"]:
                                print("Error")
                            nodes.append(fact)
                            links.append({"_from": wikidata_doc["id"], "type": prop_id, "_to": fact["id"]})
                        elif datatype == "time":
                            date = claim['mainsnak']['datavalue']['value']['time']
                            precision = claim['mainsnak']['datavalue']['value']['precision']
                            formatted_date = date_formatter.format(date, precision)
                            fact = create_time_fact(formatted_date, date)

                            nodes.append(fact)
                            links.append({"_from": wikidata_doc["id"], "type": prop_id, "_to": fact["id"]})
                        else:
                            continue

                    except KeyError:
                        continue
                    except:
                        traceback.print_exc()

        except:
            skipped += 1
            traceback.print_exc()

    elapsed = int(time.time() - start_time)
    res = {"processed": len(nodes), "elapsed": elapsed, "edges": links, "nodes": nodes, "extract_entities": entity_ids}
    return res


def export4neo(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]
    wikidocs = [x for x in wikidata.find({}, {'id': 1, '_id': 0}).sort('id') if x["id"].startswith("Q")]
    chunks = get_chunks(wikidocs, config.CHUNK_SIZE, 'id')
    wiki_size = len(wikidocs)
    del wikidocs
    start_time = time.time()
    total = 0

    if config.NUM_WORKERS == 1:
        for chunk in chunks:
            merge(chunk, {})
    else:
        pool = multiprocessing.Pool(config.NUM_WORKERS)
        with open("edges.json", "wt", encoding="utf8") as edgesf, \
            open("nodes.json", "wt", encoding="utf8") as nodesf, \
            open("entities_ids.txt", "wt", encoding="utf8") as entityf:
            with tqdm(total=wiki_size) as pbar:
                for res in pool.imap(partial(merge, configs=configs), chunks):
                    total += res['processed']
                    res['total'] = total
                    part = int(time.time() - start_time)
                    res['elapsed'] = compress(res['elapsed'])
                    res['total_elapsed'] = compress(part)
                    # logging.info("Processed {processed} ({total} in total) documents in {elapsed} (running time {"
                    #              "total_elapsed})".format(**res))

                    pbar.update(config.CHUNK_SIZE)

                    for node in res["nodes"]:
                        nodesf.write(json.dumps(node, ensure_ascii=False) + "\n")

                    for edge in res["edges"]:
                        edgesf.write(json.dumps(edge, ensure_ascii=False) + "\n")

                    for eid in res["extract_entities"]:
                        entityf.write(f"{eid}\n")

        pool.terminate()
    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {}".format(total, compress(elapsed)))
    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))
    export4neo({})
    logging.info("Completed %s", " ".join(sys.argv))
