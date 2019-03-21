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

NO_UNIT = {'label': '', 'id': ''}

BATCH_WRITE_SIZE = 500


def clean_wikidata_docs(docs: List[Dict]) -> List[Dict]:
    """
    Cleans the documents in docs lists
    :param docs:
    :return:
    """
    for doc in docs:
        try:
            yield _clean_doc(doc)
        except:
            continue


DOC_CLEAN_KEYS = ['type', 'datatype', 'descriptions', 'claims', 'labels', 'sitelinks']


def _clean_doc(doc: Dict) -> Dict:
    """
    Removes unwanted information from the document
    :param doc:
    :return:
    """
    doc['label'] = doc['labels'][config.LANG]['value']
    aliases = doc['aliases'][config.LANG] if config.LANG in doc['aliases'] else []

    doc['aliases'] = [alias['value'] for alias in aliases]

    for key in DOC_CLEAN_KEYS:
        try:
            del doc[key]
        except:
            continue
    return doc


def get_objects_id(claims: Dict) -> List:
    """
    Given a list of claims returns a list of unique wikidata ids
    :param claims:
    :return:
    """
    ids = set()
    for prop in claims:
        for claim in claims[prop]:
            try:
                datatype = claim['mainsnak']['datavalue']['type']
                if datatype == "wikibase-entityid":
                    d_id = claim['mainsnak']['datavalue']['value']['id']
                    ids.add(d_id)
                elif datatype == "quantity":
                    d_id = claim['mainsnak']['datavalue']['value']['unit'].split("/")[-1]
                    ids.add(d_id)
                else:
                    continue
            except:
                traceback.print_exc()

    return list(ids)


def documents_to_dict(documents: List[Dict]) -> Dict[str, Dict]:
    """
    Given a list of documents, returns a dict containing the ids of the documents a keys, the dict as value
    :param documents:
    :return:
    """
    res = {}
    for doc in documents:
        res[doc['id']] = doc
    return res


def create_string_fact(value: str) -> Dict:
    value = value.strip()
    fact = {"value": value, "type": "value", "id": value}
    return fact


def create_wikibase_fact(document: Dict) -> Dict:
    fact = {'value': document['label'], "type": "wikibase"}
    fact.update(document)
    return fact


def create_quantity_fact(amount: str, unit: Dict) -> Dict:
    amount = amount[1:] if amount.startswith("+") else amount
    value = amount + " " + unit['label']
    fid = amount + unit['id']
    fact = {"value": value.strip(), "type": "quantity", "id": fid}
    fact.update(unit)
    return fact


def create_time_fact(formatted_date: str, date: str):
    fact = {"value": formatted_date, "type": "date", "id": date}
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
    prop_cache = {}

    links = collections.defaultdict(list)
    nodes = {}
    n = 0
    for wikidata_doc in wikidata.find({"wikidata_id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        try:
            properties_ids = set(wikidata_doc['claims'].keys())
            uncached_prop_ids = list(properties_ids - set(prop_cache.keys()))
            prop_docs = list(wikidata.find({"id": {"$in": uncached_prop_ids}}, {"_id": 0}))
            uncached_prop = list(clean_wikidata_docs(prop_docs))
            list_prop = documents_to_dict(uncached_prop)
            prop_cache.update(list_prop)

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
                            nodes[prop_id].append(fact)
                            links[wikidata_doc["wikidata_id"]].append((prop_id, fact["id"]))
                        elif datatype == "wikibase-entityid":
                            d_id = claim['mainsnak']['datavalue']['value']['id']
                            links[wikidata_doc["wikidata_id"]].append((prop_id, d_id))
                        elif datatype == "quantity":
                            d_id = claim['mainsnak']['datavalue']['value']['unit'].split("/")[-1]
                            amount = claim['mainsnak']['datavalue']['value']['amount']
                            unit = wikidata.find({"id": d_id}, {"_id": 0})
                            if not unit:
                                unit = NO_UNIT
                            fact = create_quantity_fact(amount, unit)
                            nodes[prop_id].append(fact)
                            links[wikidata_doc["wikidata_id"]].append(fact["id"])
                        elif datatype == "time":
                            date = claim['mainsnak']['datavalue']['value']['time']
                            precision = claim['mainsnak']['datavalue']['value']['precision']
                            formatted_date = date_formatter.format(date, precision)
                            fact = create_time_fact(formatted_date, date)
                            nodes[prop_id].append(fact)
                            links[wikidata_doc["wikidata_id"]].append((prop_id, fact["id"]))
                        else:
                            continue

                    except:
                        traceback.print_exc()

        except:
            traceback.print_exc()

    if nodes:
        n += len(nodes)

    elapsed = int(time.time() - start_time)
    res = {"processed": n, "elapsed": elapsed, "edges": links, "nodes": nodes}
    return res


def export4neo(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidate = db[config.WIKIDATA_COLLECTION]
    wikidocs = list(wikidate.find({}, {'wikidata_id': 1, '_id': 0}).sort('wikidata_id'))
    chunks = get_chunks(wikidocs, config.CHUNK_SIZE, 'wikidata_id')
    del wikidocs
    start_time = time.time()
    total = 0

    if config.NUM_WORKERS == 1:
        for chunk in chunks:
            merge(chunk, {})
    else:
        pool = multiprocessing.Pool(config.NUM_WORKERS)
        with open("edges.txt", "wt", encoding="utf8") as edgesf, \
            open("nodes.txt", "wt", encoding="utf8") as nodesf:
            for res in pool.imap(partial(merge, configs=configs), chunks):
                total += res['processed']
                res['total'] = total
                part = int(time.time() - start_time)
                res['elapsed'] = compress(res['elapsed'])
                res['total_elapsed'] = compress(part)
                logging.info("Processed {processed} ({total} in total) documents in {elapsed} (running time {"
                             "total_elapsed})".format(**res))

                for node in res["nodes"]:
                    nodesf.write(json.dumps(node, ensure_ascii=False))

                for edge in res["edges"]:
                    edgesf.write(json.dumps(edge, ensure_ascii=False))


        pool.terminate()
    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {}".format(total, compress(elapsed)))
    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))
    export4neo({})
    logging.info("Completed %s", " ".join(sys.argv))
