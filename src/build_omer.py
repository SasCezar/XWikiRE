import logging
import multiprocessing
import time
from functools import partial

from natural.date import compress
from pymongo import MongoClient

import config
from utils import find_full_matches, find_matches, get_chunks


def distant_supervision(answer_sequence, entity_sequence, text_sequence, sentence_breaks):
    for start, end in zip([0] + sentence_breaks, sentence_breaks):
        sentence = text_sequence[start:end]
        # TODO If want to add aliases Cross product between aliases of answer and entity, then ANY for if statement
        if answer_sequence in sentence and entity_sequence in sentence:
            return start, end

    return False


def build(page):
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


def build_omer(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    documents_id = list(wikipedia.find({}, {"wikidata_id": 1, "_id": 0}).sort("wikidata_id"))
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


if __name__ == '__main__':
    build_omer({})
