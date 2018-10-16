import collections
import logging
import multiprocessing
import re
import sys
import traceback
from functools import partial
from typing import List, Dict, Set

from pymongo import MongoClient

import config
from date_parsers import DateFactory

logger = logging.getLogger(__name__)


STOP_SECTIONS = {
    'en': ['See also', 'Notes', 'Further reading', 'External links'],
    'fr': ['Notes et références', 'Bibliographie', 'Voir aussi', 'Annexes', 'Références'],
    'it': []
}

NO_UNIT = {'label': ''}

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
    doc['aliases'] = doc['aliases'][config.LANG] if config.LANG in doc['aliases'] else []

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
    tokens, _, _, = tokenizer.tokenize(document['label'])
    fact = {'value': document['label'], "value_sequence": tokens}
    fact.update(document)
    return fact


def create_quantity_fact(amount: str, unit: Dict) -> Dict:
    amount = amount[1:] if amount.startswith("-") else amount
    value = amount + " " + unit['label']
    fact = {"value": value.strip(), 'value_sequence': value.split()}
    fact.update(unit)
    return fact


def create_time_fact(date: str):
    fact = {"value": date, 'value_sequence': date.split()}
    return fact


def tokenize(document):
    article_text = document['text']
    tokens, break_levels, pos_tagger_tokens = tokenizer.tokenize(article_text)
    document['string_sequence'] = tokens
    document['break_levels'] = break_levels
    document['pos_tagger_sequence'] = pos_tagger_tokens
    document['sentence_breaks'] = [i for i, brk in enumerate(break_levels) if brk == 3]
    document['paragraph_breaks'] = [i for i, brk in enumerate(break_levels) if brk == 4]


def format_text(sections: List, section_titles: List) -> str:
    result = "".join((text for title, text in zip(section_titles, sections) if title not in STOP_SECTIONS))
    return result.strip()


def merge(docs, configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]

    date_formatter = DateFactory.create(config.LANG, config.LOCALE)
    prop_cache = {}

    processed_docs = []
    for page in docs:
        try:
            wikidata_doc = wikidata.find_one({"id": page['wikidata_id']}, {"_id": 0})

            properties_ids = get_properties_ids(wikidata_doc['claims'])
            uncached_prop_ids = list(properties_ids - set(prop_cache.keys()))
            prop_docs = wikidata.find({"id": {"$in": uncached_prop_ids}}, {"_id": 0})
            uncached_props = clean_wikidata_docs(prop_docs)

            for uncached_prop in uncached_props:
                tokens, _, _ = tokenizer.tokenize(uncached_prop['label'])
                uncached_prop['label_sequence'] = tokens

            prop_cache.update(documents_to_dict(uncached_props))

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
            merged_document['text'] = format_text(page['section_texts'], page['section_titles'])
            merged_document['properties'] = {pid: prop_cache[pid] for pid in facts if pid in prop_cache}
            merged_document['facts'] = facts

            tokenize(merged_document)

            processed_docs.append(merged_document)
        except:
            traceback.print_exc()

    return processed_docs


def chunkize(sequence, chunksize):
    res = []
    for j in range(0, len(sequence), chunksize):
        chunk = sequence[j:j + chunksize]
        res.append(chunk)

    return res


def merge_wikis(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    pool = multiprocessing.Pool(config.NUM_WORKERS)
    wikidocs = list(wikipedia.find({}, {"_id": 0}).sort("wikidata_id"))
    chunks = chunkize(wikidocs, chunksize=1000)
    del wikidocs
    for docs in pool.imap(partial(merge, configs=configs), chunks):
        wikimerge.insert_many(list(docs), ordered=False, bypass_document_validation=True)

    pool.terminate()

    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger.info("Running %s", " ".join(sys.argv))
    merge_wikis({})
    logger.info("Completed %s", " ".join(sys.argv[0]))
