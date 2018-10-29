import csv
import hashlib
import itertools
import logging
import multiprocessing
import re
import sys
import time
from collections import defaultdict
from functools import partial

from natural.date import compress
from pymongo import MongoClient

import config
from utils import get_chunks, is_sublist


def distant_supervision(answer_sequence, entity_sequence, text_sequence, sentence_breaks):
    for start, end in zip([-1] + sentence_breaks, sentence_breaks + [len(text_sequence) - 1]):
        sentence = text_sequence[start + 1:end + 1]
        # TODO If want to add aliases Cross product between aliases of answer and entity, then ANY for if statement
        if is_sublist(answer_sequence, sentence) and is_sublist(entity_sequence, sentence):
            return start + 1, end + 1

    return False


SEP_MAPPING = {
    0: '',
    1: ' ',
    2: '\n',
    3: ' ',
    4: '\n\n'
}


def rebuild_sentence(start, end, tokens, breaks):
    sentence = ""
    for separator, token in zip(breaks[start:end], tokens[start:end]):
        sentence += SEP_MAPPING[separator] + token

    return sentence.strip()


BATCH_WRITE_SIZE = 500


def clean_sentence(sentence):
    sentence = re.sub("\n+", " ", sentence)
    return sentence


def get_id_for_qa(page_id, prop_id, answer_id):
    unique_str = " ".join([page_id, prop_id, answer_id])
    return hashlib.sha1(unique_str.encode("utf-8")).hexdigest()


def _create_negative(a, b):
    if a['prop_id'] == b['prop_id']:
        return {}
    if not is_sublist(a['answer_sequence'], b['sentence_sequence']):
        neg_a = {"question": a['question'], "sentence": b['sentence'], "sentence_sequence": b['sentence_sequence'],
                 "answer": "", "id": get_id_for_qa(a['id'], a['prop_id'], b['id']),
                 "answer_sequence": [], "answer_id": 0, "prop_id": a['prop_id'],
                 "type": a['type'], "example": "negative", "source_a": a['id'], "source_b": b['id']}
        return neg_a
    return {}


def create_negatives(qas):
    neg_examples = []
    for type in qas:
        combinations = itertools.combinations(qas[type], 2)
        for a, b in combinations:
            negative = _create_negative(a, b)
            if negative:
                neg_examples.append(negative)

    return neg_examples


PREPOSITIONS = {
    'it': {
        'default': 'di',
        'pattern': "(?P<composed>Di|Del|Dello|Della|Dei|Degli|Delle|Dell')|(?P<article>Il|Lo|La|I|Gli|Le|L')\s?{}"
    }
}


def get_preposition(text, entity):
    pattern = PREPOSITIONS[config.LANG]['pattern'].format(entity)
    match = re.search("", text)
    if match:
        return match.group(0)
    else:
        return PREPOSITIONS[config.LANG]['default']

def build(limit, configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    omermerge = db[config.OMERMERGE_COLLECTION]

    processed = []
    n = 0
    neg_count = 0
    pos_count = 0
    start_time = time.time()
    for page in wikimerge.find({"id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        omer_doc = {"id": page['id'], "break_levels": page['break_levels'], "string_sequence": page['string_sequence'],
                    "sentence_breaks": page['sentence_breaks'], "text": page['text'],
                    "label_sequence": page['label_sequence'], "label": page['label'], 'QA': {}}

        entity_preposition = get_preposition(page['text'], page['label'])
        omer_doc['preposition'] = entity_preposition
        qas = defaultdict(list)
        for prop in page['facts']:
            question = page['properties'][prop]
            omer_doc['QA'][prop] = []

            for fact in page['facts'][prop]:
                answer_sequence = fact['value_sequence']

                match_index = distant_supervision(answer_sequence, omer_doc['label_sequence'],
                                                  omer_doc['string_sequence'], omer_doc['sentence_breaks'])

                if not match_index:
                    continue
                start, end = match_index
                sentence_sequence = omer_doc["string_sequence"][start:end]
                sentence = rebuild_sentence(start, end, omer_doc['string_sequence'], omer_doc['break_levels'])
                sentence = clean_sentence(sentence)
                qa = {"question": question['label'], "sentence": sentence, "sentence_sequence": sentence_sequence,
                      "answer": fact['value'], "id": get_id_for_qa(page['id'], prop, fact['id']),
                      "answer_sequence": answer_sequence, "answer_id": fact['id'], "prop_id": prop,
                      "type": fact['type'], "example": "positive"}

                pos_count += 1

                qas[fact['type']].append(qa)

                omer_doc['QA'][prop].append(qa)

        negative_examples = create_negatives(qas)

        neg_count += len(negative_examples)
        for example in negative_examples:
            prop = example['prop_id']
            omer_doc['QA'][prop].append(example)

        processed.append(omer_doc)

        if len(processed) > BATCH_WRITE_SIZE:
            omermerge.insert_many(processed, ordered=False, bypass_document_validation=True)
            n += len(processed)
            processed = []

    if processed:
        omermerge.insert_many(processed, ordered=False, bypass_document_validation=True)
        n += len(processed)

    elapsed = int(time.time() - start_time)
    return n, elapsed, neg_count, pos_count


def build_omer(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIMERGE_COLLECTION]
    documents_id = list(wikipedia.find({}, {"id": 1, "_id": 0}).sort("id"))
    client.close()
    start_time = time.time()
    total = 0
    tot_neg_examples = 0
    tot_pos_examples = 0
    chunks = get_chunks(documents_id, config.CHUNK_SIZE, 'id')
    if config.NUM_WORKERS == 1:
        for chunk in chunks:
            build(chunk, {})
    else:
        pool = multiprocessing.Pool(config.NUM_WORKERS)

        for n, elapsed, neg_examples, pos_examples in pool.imap(partial(build, configs=configs), chunks):
            total += n
            part = int(time.time() - start_time)
            tot_neg_examples += neg_examples
            tot_pos_examples += pos_examples
            logging.info(
                "Processed {} ({} in total) documents in {} (running time {}) - Neg examples {} - Pos examples {}".format(
                    n, total,
                    compress(
                        elapsed),
                    compress(part),
                    tot_neg_examples,
                    tot_pos_examples))

        pool.terminate()
    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {} - Neg examples {} - Pos examples {}".format(total, compress(elapsed),
                                                                                           tot_neg_examples,
                                                                                           tot_pos_examples))
    return


def extract_examples(example_type="negative"):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.OMERMERGE_COLLECTION]
    documents = wikipedia.find({}, {"QA": 1, "label": 1, "_id": 0})

    omer_props = set()
    with open("C:\\Users\sasce\PycharmProjects\WikiReading\src\\resources\omer_prop_id.txt", "rt",
              encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for pid, _ in reader:
            omer_props.add(pid)

    with open("es_qa_neg.txt", "wt", encoding="utf8", newline="") as outf:
        writer = csv.writer(outf, delimiter="\t")
        for document in documents:
            for prop in document['QA']:
                if prop not in omer_props:
                    continue
                for qa in document['QA'][prop]:
                    if example_type and qa['example'] == example_type:
                        writer.writerow([qa['id'], qa['prop_id'], qa['question'], document['label'], qa['sentence'],
                                         qa['answer']])


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))
    # build_omer({})
    extract_examples()
    logging.info("Completed %s", " ".join(sys.argv))
