import csv
from collections import Counter
from itertools import chain

from pymongo import MongoClient

import config

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


# TODO Fix vocabs creation

def build_document_vocab(collection, out_path):
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

    save_vocab(out_path, word_count)


def build_pos_vocab(collection, out_path):
    texts = collection.find({}, {"pos": 1, "_id": 0})

    pos_counter = Counter()
    for doc in texts:
        pos_counter.update(doc['pos'])

    total = 0
    for key in pos_counter:
        total += pos_counter[key]

    OTHER_TOKENS[0] = (0, ("!!!TOTAL", total))

    save_vocab(out_path, pos_counter)


def save_vocab(path, char_count):
    with open(path, "wt", encoding="utf8", newline="") as outf:
        writer = csv.writer(outf, delimiter="\t")
        for i, (token, count) in chain(OTHER_TOKENS, enumerate(char_count.most_common(), start=4)):
            writer.writerow([i, token, count])


def build_answer_vocab(collection, out_path):
    texts = collection.find({}, {"claims": 1, "_id": 0})

    answer_vocab = Counter()
    for doc in texts:
        for claim in doc['claims']:
            tokens = claim['label_sequence']
            answer_vocab.update(tokens)

    total = 0
    for key in answer_vocab:
        total += answer_vocab[key]

    OTHER_TOKENS[0] = (0, ("!!!TOTAL", total))

    save_vocab(out_path, answer_vocab)


def build_char_vocab(collection, out_path):
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

    save_vocab(out_path, char_count)


def build_vocabs():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    collection = db[config.WIKIMERGE_COLLECTION]
    build_char_vocab(collection, config.CHAR_VOCAB_OUT)
    build_document_vocab(collection, config.DOCUMENT_VOCAB_PATH)
    build_pos_vocab(collection, config.TYPE_VOCAB_PATH)
    build_answer_vocab(collection, config.ANSWER_VOCAB_PATH)


if __name__ == '__main__':
    build_vocabs()