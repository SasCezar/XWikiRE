import collections
import multiprocessing as mp
import re
import traceback
from typing import List, Dict, Set

from pymongo import MongoClient

import config

STOP_SECTIONS = {
    'en': ['See also', 'Notes', 'Further reading', 'External links'],
    'fr': ['Notes et références', 'Bibliographie', 'Voir aussi', 'Annexes', 'Références'],
    'it': []
}

NO_UNIT = {'label': ''}

FILE_RE = re.compile(
    "(\.(AVI|CSS|DOC|EXE|GIF|SVG|BMP|HTML|JPG|JPEG|MID|MIDI|MP3|MPG|MPEG|MOV|QT|PDF|PNG|RAM|RAR|TIFF|TXT|WAV|ZIP))$",
    re.IGNORECASE)

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
    tokens, _, _ = tokenizer.tokenize(document['label'])
    fact = {'value': document['label'], "value_sequence": tokens}
    fact.update(document)
    return fact


def create_quantity_fact(amount: str, unit: Dict) -> Dict:
    amount = amount[1:] if amount.startswith("-") else amount
    value = amount + " " + unit['label']
    tokens, _, _ = tokenizer.tokenize(value)
    fact = {"value": value.strip(), 'value_sequence': tokens}
    fact.update(unit)
    return fact


def create_time_fact(date: str):
    tokens, _, _ = tokenizer.tokenize(date)
    fact = {"value": date, 'value_sequence': tokens}
    return fact


def tokenize(merged_document):
    article_text = merged_document['text']
    tokens, _, break_levels = tokenizer.tokenize(article_text)
    merged_document['string_sequence'] = tokens
    merged_document['break_levels'] = break_levels

    for prop in merged_document['properties']:
        tokens, _ = tokenizer.tokenize(merged_document['properties'][prop]['label'])
        merged_document['properties'][prop]['label'] = tokens


def extract_features(merged_document: Dict) -> Dict:
    tokenize(merged_document)
    # pos_tags = pos_tagger.tag(merged_document['string_sequence'])
    # merged_document['pos_tags'] = pos_tags
    return merged_document


def format_text(sections: List, section_titles: List) -> str:
    result = "".join((text for title, text in zip(section_titles, sections) if title not in STOP_SECTIONS))
    return result.strip()


def merge_wikis(limit):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikimerge = db[config.WIKIMERGE_COLLECTION]

    date_formatter = config.DATE_FORMATTER
    prop_cache = {}

    processed_docs = []
    for page in wikipedia.find({"wikidata_id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        try:
            wikidata_doc = wikidata.find_one({"id": page['wikidata_id']}, {"_id": 0})

            properties_ids = get_properties_ids(wikidata_doc['claims'])
            uncached_prop_ids = list(properties_ids - set(prop_cache.keys()))
            prop_docs = wikidata.find({"id": {"$in": uncached_prop_ids}}, {"_id": 0})
            uncached_prop = clean_wikidata_docs(prop_docs)
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
            merged_document['text'] = format_text(page['section_texts'], page['section_titles'])
            merged_document['properties'] = {pid: prop_cache[pid] for pid in facts if pid in prop_cache}
            merged_document['facts'] = facts

            document_features = extract_features(merged_document)
            merged_document.update(document_features)

            processed_docs.append(merged_document)

            if len(processed_docs) >= BATCH_WRITE_SIZE:
                wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)
                processed_docs = []

        except:
            traceback.print_exc()

    if processed_docs:
        wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)

    return


def get_chunks(sequence, chunk_size):
    """
    Computes the lower limit and the upper limit of a collection of documents
    :param sequence:
    :param chunk_size:
    :return: The doc id for the lower and upper limits
    """
    for j in range(0, len(sequence), chunk_size):
        chunck = sequence[j:j + chunk_size]
        lower = chunck[0]['wikidata_id']
        upper = chunck[-1]['wikidata_id']
        yield (lower, upper)


if __name__ == '__main__':
    chunk_size = 5000
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    documents_id = list(wikipedia.find({}, {"wikidata_id": 1, "_id": 0}).sort("wikidata_id"))
    client.close()
    # for limit in get_chunks(documents_id, chunk_size):
    #   merge_wikis(limit)
    pool = mp.Pool(processes=6)
    pool.map(merge_wikis, get_chunks(documents_id, chunk_size))
    pool.close()
    pool.join()
