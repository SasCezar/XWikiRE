import collections
import logging
import multiprocessing
import re
import sys
import time
import traceback
from functools import partial
from typing import List, Dict, Set

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

STOP_SECTIONS_RE = re.compile("===?\s({})\s===?".format('|'.join(STOP_SECTIONS[config.LANG])))

NO_UNIT = {'label': ''}

BATCH_WRITE_SIZE = 500
tokenizer = config.TOKENIZER


def get_properties_ids(claims: Dict) -> Set:
    """
    Returns a set of properties keys
    :param claims:
    :return:
    """
    return set(claims.keys())


def clean_wikidata_docs(docs: List[Dict]) -> List[Dict]:
    """
    Cleans the documents in docs lists
    :param docs:
    :return:
    """
    clean_docs = []
    for doc in docs:
        try:
            clean_doc = _clean_doc(doc)
            clean_docs.append(clean_doc)
        except:
            continue
    return clean_docs


DOC_CLEAN_KEYS = ['type', 'datatype', 'descriptions', 'claims', 'labels', 'sitelinks']


def _clean_doc(doc: Dict) -> Dict:
    """
    Removes unwanted information from the document
    :param doc:
    :return:
    """
    doc['label'] = doc['labels'][config.LANG]['value']
    doc['aliases'] = doc['aliases'].get(config.LANG, [])

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


def create_wikibase_fact(document: Dict) -> Dict:
    fact = {'value': document['label']}
    fact.update(document)
    return fact


def create_quantity_fact(amount: str, unit: Dict) -> Dict:
    amount = amount[1:] if amount.startswith("-") else amount
    value = amount + " " + unit['label']
    fact = {"value": value.strip()}
    fact.update(unit)
    return fact


def create_time_fact(date: str):
    fact = {"value": date}
    return fact


def tokenize(document):
    article_text = document['text']
    tokens, break_levels, pos_tagger_tokens = tokenizer.tokenize(article_text)
    document['string_sequence'] = tokens
    tokens, _, _ = tokenizer.tokenize(document['label'])
    document['label_sequence'] = tokens
    document['break_levels'] = break_levels
    document['pos_tagger_sequence'] = pos_tagger_tokens
    document['sentence_breaks'] = [i for i, brk in enumerate(break_levels) if brk == 3]
    document['paragraph_breaks'] = [i for i, brk in enumerate(break_levels) if brk == 4]

    for prop in document['facts']:
        for fact in document['facts'][prop]:
            tokens, _, _ = tokenizer.tokenize(fact['value'])
            fact['value_sequence'] = tokens


def clean_text(text: str) -> str:
    match = STOP_SECTIONS_RE.search(text)
    if match and match.start() > 0:
        text = text[:match.start()].strip()
    text = re.sub("===?\s[^=]+\s===?\n?", "", text)
    text = re.sub("\[\d+\]", "", text)
    text = re.sub("\n{3,}", "\n\n", text)
    tokens, _, _ = tokenizer.tokenize(text)
    return text


def tokenize_props(props):
    for prop in props:
        tokens, _, _ = tokenizer.tokenize(prop['label'])
        prop['label_sequence'] = tokens
        # for alias in prop['aliases']:
        #    tokens, _, _ = tokenizer.tokenize(prop['value'])
        #    alias['value_sequence'] = tokens


def merge(limit, configs):
    start_time = time.time()
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikimerge = db[config.WIKIMERGE_COLLECTION]

    date_formatter = config.DATE_FORMATTER
    prop_cache = {}

    processed_docs = []
    n = 0
    for page in wikipedia.find({"wikidata_id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        try:
            wikidata_doc = wikidata.find_one({"id": page['wikidata_id']}, {"_id": 0})

            properties_ids = get_properties_ids(wikidata_doc['claims'])
            uncached_prop_ids = list(properties_ids - set(prop_cache.keys()))
            prop_docs = wikidata.find({"id": {"$in": uncached_prop_ids}}, {"_id": 0})
            uncached_prop = clean_wikidata_docs(prop_docs)
            tokenize_props(uncached_prop)
            prop_cache.update(documents_to_dict(uncached_prop))

            object_documents_ids = get_objects_id(wikidata_doc['claims'])
            object_documents = wikidata.find({"id": {"$in": object_documents_ids}}, {"_id": 0})
            documents_dict = documents_to_dict(clean_wikidata_docs(object_documents))

            facts = collections.defaultdict(list)
            for prop_id in wikidata_doc['claims']:
                for claim in wikidata_doc['claims'][prop_id]:
                    try:
                        datatype = claim['mainsnak']['datavalue']['type']
                        if datatype == "wikibase-entityid":
                            d_id = claim['mainsnak']['datavalue']['value']['id']
                            if d_id in documents_dict:
                                document = documents_dict[d_id]
                                fact = create_wikibase_fact(document)
                                facts[prop_id].append(fact)
                        elif datatype == "quantity":
                            d_id = claim['mainsnak']['datavalue']['value']['unit'].split("/")[-1]
                            amount = claim['mainsnak']['datavalue']['value']['amount']
                            unit = documents_dict[d_id] if d_id in documents_dict else NO_UNIT
                            fact = create_quantity_fact(amount, unit)
                            facts[prop_id].append(fact)
                        elif datatype == "time":
                            date = claim['mainsnak']['datavalue']['value']['time']
                            precision = claim['mainsnak']['datavalue']['value']['precision']
                            formatted_date = date_formatter.format(date, precision)
                            fact = create_time_fact(formatted_date)
                            facts[prop_id].append(fact)
                        else:
                            continue
                    except:
                        traceback.print_exc()

            merged_document = _clean_doc(wikidata_doc)
            merged_document['text'] = clean_text(page['text'])
            merged_document['properties'] = {pid: prop_cache[pid] for pid in facts if pid in prop_cache}
            merged_document['facts'] = facts

            tokenize(merged_document)

            processed_docs.append(merged_document)

            if len(processed_docs) >= BATCH_WRITE_SIZE:
                n += len(processed_docs)
                wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)
                processed_docs = []

        except:
            traceback.print_exc()

    if processed_docs:
        n += len(processed_docs)
        wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)

    elapsed = int(time.time() - start_time)
    return n, elapsed


def wikimerge(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikidocs = list(wikipedia.find({}, {'wikidata_id': 1, '_id': 0}).sort('wikidata_id'))
    chunks = get_chunks(wikidocs, config.CHUNK_SIZE, 'wikidata_id')
    del wikidocs
    start_time = time.time()
    total = 0
    pool = multiprocessing.Pool(config.NUM_WORKERS)
    for n, elapsed in pool.map(partial(merge, configs=configs), chunks):
        total += n
        part = int(time.time() - start_time)
        logging.info("Processed {} ({} in total) documents in {} (running time {})".format(n, total, compress(elapsed),
                                                                                           compress(part)))

    pool.terminate()
    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {}".format(total, compress(elapsed)))
    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)

    logging.info("Running %s", " ".join(sys.argv))
    wikimerge({})
    logging.info("Completed %s", " ".join(sys.argv))
