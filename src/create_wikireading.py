import multiprocessing
from functools import partial

from pymongo import MongoClient

import config
from vocabs import load_vocab


def find_sub_list(sublist, list):
    results = []
    sll = len(sublist)
    for ind in (i for i, e in enumerate(list) if e == sublist[0]):
        if list[ind:ind + sll] == sublist:
            results.append(range(ind, ind + sll))

    return results


def find_full_matches(sequence, answer):
    return find_sub_list(answer, sequence)


def find_matches(sequence, answer):
    elements = set(answer)
    return [index for index, value in enumerate(sequence) if value in elements]


def build(docs, configs):
    answer_vocab = load_vocab(config.ANSWER_VOCAB_PATH)
    document_vocab = load_vocab(config.DOCUMENT_VOCAB_PATH)
    raw_answers_vocab = load_vocab(config.RAW_ANSWER_VOCAB_PATH)
    type_vocab = load_vocab(config.TYPE_VOCAB_PATH)

    processed_docs = []
    for page in docs:
        wikireading_doc = {"key": page['id'],
                           "break_levels": page['break_levels'],
                           "string_sequence": page['string_sequence'],
                           "paragraph_breaks": page['paragraph_breaks'],
                           "sentence_breaks": page['sentence_breaks'],
                           "text": page['text'],
                           "pos_tags": page['pos_tags'],
                           'type_sequence': [type_vocab[pos] for pos in page['pos_tags']],
                           'document_sequence': [document_vocab[word] for word in page['string_sequence']]
                           }

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

            processed_docs.append(wikireading_doc)

    return processed_docs


def chunkize(sequence, chunksize):
    res = []
    for j in range(0, len(sequence), chunksize):
        chunk = sequence[j:j + chunksize]
        res.append(chunk)

    return res


def make_wikireading(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikireading = db[config.WIKIREADING_COLLECTION]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    pool = multiprocessing.Pool(config.NUM_WORKERS)
    wikidocs = list(wikimerge.find({}, {"_id": 0}).sort("id"))
    chunks = chunkize(wikidocs, chunksize=1000)
    del wikidocs
    for docs in pool.map(partial(build, configs=configs), chunks):
        wikireading.insert_many(docs, ordered=False, bypass_document_validation=True)

    pool.terminate()

    return
