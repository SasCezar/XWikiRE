import hashlib
import logging
import multiprocessing
import re
import sys
import time
from collections import defaultdict
from functools import partial

import nltk
from natural.date import compress
from pymongo import MongoClient

import config
from utils import get_chunks, is_sublist


def distant_supervision(answer_sequence, entity_sequence, text_sequence, sentence_breaks):
    if not answer_sequence:
        return False

    for start, end in zip([-1] + sentence_breaks, sentence_breaks + [len(text_sequence) - 1]):
        sentence = text_sequence[start + 1:end + 1]
        # TODO If want to add aliases Cross product between aliases of answer and entity, then ANY for if statement
        if is_sublist(answer_sequence, sentence) and is_sublist(entity_sequence, sentence):
            return start + 1, end + 1

    return False


BATCH_WRITE_SIZE = 500


def get_id_for_qa(page_id, prop_id, answer_id):
    unique_str = " ".join([page_id, prop_id, answer_id])
    return hashlib.sha1(unique_str.encode("utf-8")).hexdigest()


def string_distant_supervision(answer, entity, sentences):
    e_template = "\\b" + re.escape(entity) + "\\b"
    a_template = "\\b" + re.escape(answer) + "\\b"
    for sentence in sentences:
        if re.search(e_template, sentence) and re.search(a_template, sentence):
            return sentence

    return False


def build(limit, configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    omermerge = db[config.OMERMERGE_COLLECTION]

    processed = []
    n = 0
    extracted = 0
    start_time = time.time()
    for page in wikimerge.find({"id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        text = page['text'].strip()
        if not text:
            continue

        sentences = nltk.tokenize.sent_tokenize(text.replace("\n\n", "\n"), language='english')
        omer_doc = {"id": page['id'], "string_sequence": page.get('string_sequence', []),
                    "text": page['text'],
                    "label_sequence": page.get('label_sequence', []), "label": page['label'], 'QA': {}}

        qas = defaultdict(list)
        for prop in page['facts']:
            relation = page['properties'][prop]
            omer_doc['QA'][prop] = []

            for fact in page['facts'][prop]:
                answer = fact['value']

                sentence = string_distant_supervision(answer, omer_doc['label'], sentences)

                if not sentence:
                    continue

                qa = {"relation": relation['label'], "sentence": sentence,
                      "answer": fact['value'], "id": get_id_for_qa(page['id'], prop, fact['id']),
                      "answer_id": fact['id'], "prop_id": prop,
                      "type": fact['type'], "example": "positive"}

                extracted += 1

                qas[fact['type']].append(qa)

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
    res = {"processed": n, "elapsed": elapsed, "extracted": extracted}
    return res


def build_srt(configs):
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

        for res in pool.imap(partial(build, configs=configs), chunks):
            total += res['processed']
            res['total'] = total
            elapsed = int(time.time() - start_time)
            res['total_elapsed'] = compress(elapsed)
            res['elapsed'] = compress(res['elapsed'])
            logging.info("Processed {processed} ({total} in total) documents in {elapsed} (running time {"
                         "total_elapsed}) - Extracted {extracted}".format(**res))

        pool.terminate()
    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {} - Neg examples {} - Pos examples {}".format(total, compress(elapsed),
                                                                                           tot_neg_examples,
                                                                                           tot_pos_examples))
    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))
    build_srt({})
    logging.info("Completed %s", " ".join(sys.argv))
