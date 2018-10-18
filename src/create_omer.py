import collections
import multiprocessing as mp
import traceback
from typing import List, Dict, Set

from pymongo import MongoClient

import config
from utils import find_full_matches, find_matches, get_chunks

STOP_SECTIONS = {
    'en': ['See also', 'Notes', 'Further reading', 'External links'],
    'fr': ['Notes et références', 'Bibliographie', 'Voir aussi', 'Annexes', 'Références'],
    'it': []
}

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
    tokens, _, _ = tokenizer.tokenize(article_text)
    document['title_sequence'] = tokens
    document['break_levels'] = break_levels
    document['pos_tagger_sequence'] = pos_tagger_tokens
    document['sentence_breaks'] = [i for i, brk in enumerate(break_levels) if brk == 3]
    document['paragraph_breaks'] = [i for i, brk in enumerate(break_levels) if brk == 4]

    for prop in document['properties']:
        tokens, _, _ = tokenizer.tokenize(document['properties'][prop]['label'])
        document['properties'][prop]['label_sequence'] = tokens


def distant_supervision(answer_sequence, entity_sequence, text_sequence, sentence_breaks):
    for start, end in zip([0] + sentence_breaks, sentence_breaks):
        sentence = text_sequence[start:end]
        # TODO If want to add aliases Cross product between aliases of answer and entity, then ANY for if statement
        if answer_sequence in sentence and entity_sequence in sentence:
            return start, end

    return False


def extract_omer(page):
    omer_doc = {"key": page['id'], "break_levels": page['break_levels'],
                "string_sequence": page['string_sequence'], "paragraph_breaks": page['paragraph_breaks'],
                "sentence_breaks": page['sentence_breaks'], "text": page['text'],
                "entity_sequence": page['title_sequence'], "entity": page['title']}

    for prop in page['facts']:
        question = page['properties'][prop]
        omer_doc['question_string_sequence'] = question['label_sequence']
        answer_string_sequence = []
        answer_breaks = []
        raw_answers = []
        full_match_answer_location = []
        answer_location = []
        for fact in page['facts'][prop]:
            if answer_string_sequence:
                answer_breaks.append(len(answer_string_sequence))
            raw_answers.append(fact['value'])
            answer_sequence = fact['value_sequence']
            answer_string_sequence += answer_sequence
            full_match_answer_location.append(
                find_full_matches(omer_doc["string_sequence"], answer_sequence))
            answer_location.append(find_matches(omer_doc["string_sequence"], answer_sequence))

            indexes = distant_supervision(answer_sequence, omer_doc['entity_sequence'],
                                          omer_doc['string_sequence'], omer_doc['sentence_breaks'])

        omer_doc['answer_string_sequence'] = answer_string_sequence
        omer_doc['raw_answer_ids'] = raw_answers
        omer_doc['answer_breaks'] = answer_breaks
        omer_doc['full_match_answer_location'] = full_match_answer_location

    return


def merge_wikis(args):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikidata = db[config.WIKIDATA_COLLECTION]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    omermerge = db[config.OMERWIKI_COLLECTION]

    date_formatter = config.DATE_FORMATTER
    prop_cache = {}

    processed_docs = []
    omer_processed = []
    for page in wikipedia.find({"wikidata_id": {"$gte": args[0], "$lte": args[1]}}, {"_id": 0}):
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
            merged_document['text'] = page['text']
            merged_document['title'] = page['title']
            merged_document['properties'] = {pid: prop_cache[pid] for pid in facts if pid in prop_cache}
            merged_document['facts'] = facts

            tokenize(merged_document)

            processed_docs.append(merged_document)

            omer_document = extract_omer(merged_document)

            omer_processed += omer_document

            if len(processed_docs) >= BATCH_WRITE_SIZE:
                wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)
                omermerge.insert_many(omer_processed, ordered=False, bypass_document_validation=True)
                processed_docs = []
                omer_processed = []


        except:
            traceback.print_exc()

    if processed_docs:
        wikimerge.insert_many(processed_docs, ordered=False, bypass_document_validation=True)

    return


def wikimerge():
    chunk_size = config.CHUNK_SIZE
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    documents_id = list(wikipedia.find({}, {"wikidata_id": 1, "_id": 0}).sort("wikidata_id"))
    client.close()
    if config.NUM_WORKERS == 1:
        for limit in get_chunks(documents_id, chunk_size, 'wikidata_id'):
            merge_wikis(limit)
    else:
        pool = mp.Pool(processes=config.NUM_WORKERS)
        pool.map(merge_wikis, get_chunks(documents_id, chunk_size, 'wikidata_id'))
        pool.close()
        pool.join()


if __name__ == '__main__':
    wikimerge()
