import collections
import datetime
import string
import traceback
import unicodedata
import multiprocessing as mp
from typing import List, Dict, Set

import nltk
from dateutil.parser import *
from nltk.tokenize.util import align_tokens
from pymongo import MongoClient


import config
from utils import get_chunks

FACT_DELIMITER_TOKEN = " "

SECTION_DELIMITER_TOKEN = "\n\n"

BCE_TOKEN = " BC"
BC_DATE_FORMAT = '%d %B %Y'

SEPARATOR_TOKENS = {
    " ": 1,
    "\n": 2,
    "SENTENCE_SEPARATOR": 3,
    "\n\n": 4,
}

BATCH_DUMP_SIZE = 500


def get_properties_ids(wikidata_doc: Dict) -> Set:
    return set(wikidata_doc['claims'].keys())


def clean_wikidata_docs(docs: List[Dict]) -> List[Dict]:
    clean_docs = []
    for doc in docs:
        clean_doc = _clean_doc(doc)
        clean_docs.append(clean_doc)

    return clean_docs


def _clean_doc(doc: Dict) -> Dict:
    del doc['type']
    del doc['datatype']
    del doc['descriptions']
    del doc['claims']
    doc['label_en'] = doc['labels']['en']['value']
    doc['label'] = doc['labels'][config.LANG]['value']
    del doc['labels']
    doc['aliases'] = doc['aliases'][config.LANG]
    return doc


def get_objects_id(claims: Dict) -> List:
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
                elif datatype == "time":
                    d_id = claim['mainsnak']['datavalue']['value']['calendarmodel'].split("/")[-1]
                    ids.add(d_id)
                else:
                    continue
            except:
                traceback.print_exc()

    return list(ids)


def documents_to_dict(documents: List[Dict]) -> Dict[Dict]:
    res = {}
    for doc in documents:
        res[doc['id']] = _clean_doc(doc)
    return res


def create_wikibase_fact(document: Dict) -> Dict:
    return document


def create_quantity_fact(amount: str, unit: Dict) -> Dict:
    fact = {"value": amount + FACT_DELIMITER_TOKEN + unit['label']}
    fact.update(unit)
    return fact


def _parse_bce_date(date: datetime, calendar_model: Dict) -> str:
    return date.year + BCE_TOKEN


def _parse_ce_date(date: datetime, calendar_model: Dict) -> str:
    return date.strftime(BC_DATE_FORMAT)


def create_time_fact(date: str, calendar_model: Dict):
    parsed_date = parse(date[1:])
    if date.startswith("-"):
        fact = {"value": _parse_bce_date(parsed_date, calendar_model)}
    else:
        fact = {"value": _parse_ce_date(parsed_date, calendar_model)}
    return fact


def get_break_levels(article_text, tokens):
    spans = align_tokens(tokens, article_text)

    breaks = [0]
    for i in range(1, len(spans)):
        if spans[i - 1][1] == spans[i][0]:
            breaks.append(0)
        else:
            sep_token = article_text[spans[i - 1][1]:spans[i - 1][1]]
            if sep_token in SEPARATOR_TOKENS:
                breaks.append(SEPARATOR_TOKENS[sep_token])
            elif sep_token in string.punctuation:
                breaks.append(SEPARATOR_TOKENS["SENTENCE_SEPARATOR"])
            elif len(sep_token) == 1 and unicodedata.category(sep_token).startswith("P"):
                breaks.append(SEPARATOR_TOKENS["SENTENCE_SEPARATOR"])
            else:
                for token in sep_token:
                    if token in SEPARATOR_TOKENS:
                        try:
                            breaks.append(SEPARATOR_TOKENS[token])
                        except:
                            breaks.append(0)
                            traceback.print_exc()

    return breaks


def tokenizer(text):
    return nltk.word_tokenize(text, config.EXT_LANG)


def tokenize(merged_document):
    article_text = merged_document['text']
    tokens = tokenizer(article_text)
    merged_document['string_sequence'] = tokens
    break_levels = get_break_levels(article_text, tokens)
    merged_document['break_levels'] = break_levels

    for property in merged_document['properties']:
        merged_document['properties'][property]['label'] = tokenizer(merged_document['properties'][property]['label'])

    for prop in merged_document['facts']:
        for fact in merged_document['facts'][prop]:
            fact['value_sequence'] = tokenizer(fact['value'])


def pos_tag_text(merged_document):
    pass


def extract_features(merged_document: Dict) -> Dict:
    tokenize(merged_document)
    # pos_tags = pos_tag_text(merged_document['string_sequence'])
    # merged_document['pos_tags'] = pos_tags
    return merged_document


def format_text(sections: List) -> str:
    return SECTION_DELIMITER_TOKEN.join(sections)


def merge_wikis():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikimerge = db[config.WIKIMERGE_COLLECTION]

    wikidata_id_ref = "labels.{}.value".format(config.LANG)
    prop_cache = {}

    processed_docs = []
    for page in wikipedia.find({"text"}):
        try:
            label = page['title'].lower()
            wikidata_doc = wikidata.find_one({wikidata_id_ref: label}, {"_id": 0})

            properties_ids = get_properties_ids(wikidata_doc['claims'])
            uncached_prop_ids = list(properties_ids - set(prop_cache.keys()))
            uncached_prop = clean_wikidata_docs(list(wikidata.find({"id": {"$in": uncached_prop_ids}}, {"_id": 0})))
            prop_cache.update(documents_to_dict(uncached_prop))

            object_documents_ids = get_objects_id(wikidata_doc['claims'])
            object_documents = wikidata.find({"id": {"$in": object_documents_ids}}, {"_id": 0})
            documents_dict = documents_to_dict(object_documents)

            facts = collections.defaultdict(list)
            clean_props = {}
            for prop_id in wikidata_doc['claims']:
                clean_props[prop_id] = prop_cache[prop_id]
                for claim in wikidata_doc['claims'][prop_id]:
                    try:
                        datatype = claim['mainsnak']['datavalue']['type']
                        if datatype == "wikibase-entityid":
                            d_id = claim['mainsnak']['datavalue']['value']['id']
                            document = documents_dict[d_id]
                            fact = create_wikibase_fact(document)
                            facts[prop_id].append(fact)
                        elif datatype == "quantity":
                            d_id = claim['mainsnak']['datavalue']['value']['unit'].split("/")[-1]
                            amount = claim['mainsnak']['datavalue']['value']['amount']
                            unit = documents_dict[d_id]
                            fact = create_quantity_fact(amount, unit)
                            facts[prop_id].append(fact)
                        elif datatype == "time":
                            d_id = claim['mainsnak']['datavalue']['value']['calendarmodel'].split("/")[-1]
                            date = claim['mainsnak']['datavalue']['value']['amount']
                            calendar_model = documents_dict[d_id]
                            fact = create_time_fact(date, calendar_model)
                            facts[prop_id].append(fact)
                        else:
                            continue
                    except:
                        traceback.print_exc()

            merged_document = _clean_doc(wikidata_doc)
            merged_document['text'] = format_text(page['section_texts'])
            merged_document['properties'] = clean_props
            merged_document['facts'] = facts

            document_features = extract_features(merged_document)
            merged_document.update(document_features)

            processed_docs.append(merged_document)

            if len(processed_docs) >= BATCH_DUMP_SIZE:
                wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)
                processed_docs = []

        except:
            traceback.print_exc()

    if processed_docs:
        wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)

    return


if __name__ == '__main__':
    chunk_size = 100000
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    documents_id = list(wikipedia.find({}, {"wikibase_item": 1, "_id": 0}).sort("wikibase_item"))
    client.close()
    pool = mp.Pool(processes=config.NUM_WORKERS)
    pool.map(merge_wikis(), get_chunks(documents_id, chunk_size))
    pool.close()
    pool.join()