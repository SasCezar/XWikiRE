import logging
import multiprocessing
import time
from functools import partial

from natural.date import compress
from pymongo import MongoClient

import config
from vocabs import load_vocab
from utils import find_full_matches, find_matches, get_chunks


def build(limit):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikimerge = db[config.WIKIMERGE_COLLECTION]

    answer_vocab = load_vocab(config.ANSWER_VOCAB_PATH)
    document_vocab = load_vocab(config.DOCUMENT_VOCAB_PATH)
    raw_answers_vocab = load_vocab(config.RAW_ANSWER_VOCAB_PATH)
    # type_vocab = load_vocab(config.TYPE_VOCAB_PATH)

    for page in wikimerge.find({"id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        wikireading_doc = {"key": page['id'], "break_levels": page['break_levels'],
                           "string_sequence": page['string_sequence'], "paragraph_breaks": page['paragraph_breaks'],
                           "sentence_breaks": page['sentence_breaks'], "text": page['text']}

        # TODO Add POS Source
        # pos_sequence = poscoll.find_one({"id": page['id']}, {"_id": 0})
        # wikireading_doc['type_sequence'] = [type_vocab[pos] for pos in pos_sequence]
        wikireading_doc['document_sequence'] = [document_vocab[word] for word in wikireading_doc['string_sequence']]

        for prop in page['facts']:
            question = page['properties'][prop]
            wikireading_doc['question_string_sequence'] = question['label_sequence']
            wikireading_doc['question_sequence'] = [document_vocab[token] for token in question['label_sequence']]
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
                    find_full_matches(wikireading_doc["string_sequence"], answer_sequence))
                answer_location.append(find_matches(wikireading_doc["string_sequence"], answer_sequence))

            wikireading_doc['answer_sequence'] = [answer_vocab[token] for token in answer_string_sequence]
            wikireading_doc['answer_string_sequence'] = answer_string_sequence
            wikireading_doc['raw_answers'] = [raw_answers_vocab[token] for token in raw_answers]
            wikireading_doc['raw_answer_ids'] = raw_answers
            wikireading_doc['answer_breaks'] = answer_breaks
            wikireading_doc['full_match_answer_location'] = full_match_answer_location


def make_wikireading(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    documents_id = list(wikimerge.find({}, {"id": 1, "_id": 0}).sort("id"))
    client.close()
    start_time = time.time()
    total = 0
    pool = multiprocessing.Pool(config.NUM_WORKERS)
    chunks = get_chunks(documents_id, config.CHUNK_SIZE, 'wikidata_id')
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
