import hashlib
import logging
import multiprocessing
import re
import time
from functools import partial

from natural.date import compress
from pymongo import MongoClient

import config
from utils import get_chunks, is_sublist


def distant_supervision(answer_sequence, entity_sequence, text_sequence, sentence_breaks):
    for start, end in zip([0] + sentence_breaks, sentence_breaks):
        sentence = text_sequence[start:end]
        # TODO If want to add aliases Cross product between aliases of answer and entity, then ANY for if statement
        if is_sublist(answer_sequence, sentence) and is_sublist(entity_sequence, sentence):
            return start, end

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


def build(limit, configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    omermerge = db[config.OMERMERGE_COLLECTION]

    processed = []
    n = 0
    start_time = time.time()
    for page in wikimerge.find({"id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        omer_doc = {"id": page['id'], "break_levels": page['break_levels'], "string_sequence": page['string_sequence'],
                    "sentence_breaks": page['sentence_breaks'], "text": page['text'],
                    "label_sequence": page['label_sequence'], "label": page['label'], 'QA': {}}

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
                sentence = rebuild_sentence(start, end, omer_doc['string_sequence'], omer_doc['break_levels'])
                sentence = clean_sentence(sentence)
                qa = {"question": question['label'], "sentence": sentence, "answer": fact['value'],
                      "id": get_id_for_qa(page['id'], prop, fact['id'])}

                omer_doc['QA'][prop].append(qa)

        processed.append(omer_doc)

        if len(processed) > BATCH_WRITE_SIZE:
            omermerge.insert_many(processed, ordered=False, bypass_document_validation=True)
            n += len(processed)
            processed = []

    if processed:
        omermerge.insert_many(processed, ordered=False, bypass_document_validation=True)
        n += len(processed)

    elapsed = int(time.time() - start_time)
    return n, elapsed


def build_omer(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIMERGE_COLLECTION]
    documents_id = list(wikipedia.find({}, {"id": 1, "_id": 0}).sort("id"))
    client.close()
    start_time = time.time()
    total = 0
    chunks = get_chunks(documents_id, config.CHUNK_SIZE, 'id')
    if config.NUM_WORKERS == 1:
        for chunk in chunks:
            build(chunk, {})
    else:
        pool = multiprocessing.Pool(config.NUM_WORKERS)
        for n, elapsed in pool.map(partial(build, configs=configs), chunks):
            total += n
            part = int(time.time() - start_time)
            logging.info("Processed {} ({} in total) documents in {} (running time {})".format(n, total,
                                                                                               compress(elapsed),
                                                                                               compress(part)))

        pool.terminate()
    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {}".format(total, compress(elapsed)))
    return


if __name__ == '__main__':
    build_omer({})
