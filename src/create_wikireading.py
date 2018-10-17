import csv
import multiprocessing as mp
from collections import Counter
from itertools import chain

from pymongo import MongoClient

import config

from utils import find_full_matches, find_matches

OTHER_TOKENS = [(0, ("!!!TOTAL", 0)),
                (0, ("!!!FLAGS", 0)),
                (0, ("<S>", 0)),
                (1, ("</S>", 0)),
                (2, ("<UNK>", 0)),
                (3, ("<NONE>", 0))]


def load_vocab(path):
    vocab = {}
    with open(path, "rt", encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for key, token, count in reader:
            vocab[key] = token

    return vocab


def build_document_vocab(collection):
    source = "string_sequence"
    texts = collection.find({}, {source: 1, "claims": 1, "_id": 0})

    word_count = Counter()
    for doc in texts:
        tokens = doc[source]
        word_count.update(tokens)
        for prop_id in doc['facts']:
            for fact in doc['facts'][prop_id]:
                tokens = fact['value_sequence']
                word_count.update(tokens)

    total = 0
    for key in word_count:
        total += word_count[key]

    OTHER_TOKENS[0] = (0, ("!!!TOTAL", total))

    save_vocab(config.DOCUMENT_VOCAB_PATH, word_count)


def build_pos_vocab(collection):
    texts = collection.find({}, {"pos": 1, "_id": 0})

    pos_counter = Counter()
    for doc in texts:
        pos_counter.update(doc['pos'])

    total = 0
    for key in pos_counter:
        total += pos_counter[key]

    OTHER_TOKENS[0] = (0, ("!!!TOTAL", total))

    save_vocab(config.TYPE_VOCAB_PATH, pos_counter)


def save_vocab(path, char_count):
    with open(path, "wt", encoding="utf8", newline="") as outf:
        writer = csv.writer(outf, delimiter="\t")
        for i, (token, count) in chain(OTHER_TOKENS, enumerate(char_count.most_common(), start=4)):
            writer.writerow([i, token, count])


def build_answer_vocab(collection):
    texts = collection.find({}, {"claims": 1, "_id": 0})

    answer_vocab = Counter()
    for doc in texts:
        for prop in doc['facts']:
            for fact in doc['facts'][prop]:
                answer_vocab.update(fact['value_sequence'])

    total = 0
    for key in answer_vocab:
        total += answer_vocab[key]

    OTHER_TOKENS[0] = (0, ("!!!TOTAL", total))

    save_vocab(config.ANSWER_VOCAB_PATH, answer_vocab)


def build_char_vocab(collection):
    texts = collection.find({}, {"text": 1, "_id": 0})

    char_count = Counter()
    for doc in texts:
        char_count.update(list(doc['text']))
        for prop in doc['facts']:
            prop_label = doc['properties'][prop]['label']
            fact_label = doc['facts'][prop]['value']
            char_count.update(list(prop_label))
            char_count.update(list(fact_label))

    total = 0
    for key in char_count:
        total += char_count[key]

    OTHER_TOKENS[0] = (0, ("!!!TOTAL", total))

    save_vocab(config.CHAR_VOCAB_OUT, char_count)


def build_vocabs():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    collection = db[config.WIKIMERGE_COLLECTION]
    build_char_vocab(collection)
    build_document_vocab(collection)
    build_pos_vocab(collection)
    build_answer_vocab(collection)


def build(limit):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    poscoll = db[config.POS_COLLECTION]
    wikimerge = db[config.WIKIMERGE_COLLECTION]

    answer_vocab = load_vocab(config.ANSWER_VOCAB_PATH)
    document_vocab = load_vocab(config.DOCUMENT_VOCAB_PATH)
    raw_answers_vocab = load_vocab(config.RAW_ANSWER_VOCAB_PATH)
    type_vocab = load_vocab(config.TYPE_VOCAB_PATH)

    for page in wikimerge.find({"id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        wikireading_doc = {"key": page['id'], "break_levels": page['break_levels'],
                           "string_sequence": page['string_sequence'], "paragraph_breaks": page['paragraph_breaks'],
                           "sentence_breaks": page['sentence_breaks'], "text": page['text']}

        pos_sequence = poscoll.find_one({"id": page['id']}, {"_id": 0})
        wikireading_doc['type_sequence'] = [type_vocab[pos] for pos in pos_sequence]
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


def get_chunks(sequence, chunk_size):
    """
    Computes the lower limit and the upper limit of a collection of documents
    :param sequence:
    :param chunk_size:
    :return: The doc id for the lower and upper limits
    """
    for j in range(0, len(sequence), chunk_size):
        chunck = sequence[j:j + chunk_size]
        lower = chunck[0]['id']
        upper = chunck[-1]['id']
        yield (lower, upper)


def make_wikireading():
    chunk_size = config.CHUNK_SIZE
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikimerge = db[config.WIKIMERGE_COLLECTION]
    documents_id = list(wikimerge.find({}, {"id": 1, "_id": 0}).sort("id"))
    client.close()
    pool = mp.Pool(processes=config.NUM_WORKERS)
    pool.map(build, get_chunks(documents_id, chunk_size))
    pool.close()
    pool.join()
