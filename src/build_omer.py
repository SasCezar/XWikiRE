import csv
import hashlib
import itertools
import json
import logging
import multiprocessing
import re
import sys
import time
import traceback
from collections import defaultdict
from functools import partial

import nltk
from natural.date import compress
from pymongo import MongoClient

import config
from article_extractors import ArticleExtractorFactory
from template_fillers import TemplateFillerFactory
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


SEP_MAPPING = {
    0: '',
    1: ' ',
    2: '\n',
    3: ' ',
    4: '\n\n'
}

article_extractor = ArticleExtractorFactory.make_extractor(config.LANG)


def rebuild_sentence(start, end, tokens, breaks):
    sentence = ""
    for separator, token in zip(breaks[start:end], tokens[start:end]):
        if not token:
            sentence += " "
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
    e_template = "\\b" + re.escape(a['answer']) + "\\b"
    if not re.search(e_template, b['sentence']):
        neg_a = {"relation": a['relation'], "sentence": b['sentence'],
                 "answer": "", "id": get_id_for_qa(a['id'], a['prop_id'], b['id']),
                 "answer_id": 0, "prop_id": a['prop_id'],
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


tokenizer = config.TOKENIZER


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
    neg_count = 0
    pos_count = 0
    start_time = time.time()
    for page in wikimerge.find({"id": {"$gte": limit[0], "$lte": limit[1]}}, {"_id": 0}):
        text = page['text'].strip()
        if not text:
            continue
        # table = str.maketrans({"\t": "\\t", "\r": "\\r", "\f": "\\f", "\b": "\\b"})
        # text = text.translate(table).strip()
        # text = re.sub("(\n|\s){2,}", "\n\n", text)
        # text = re.sub("\n{3,}", "\n\n", text)
        sentences = nltk.tokenize.sent_tokenize(text, language='french')
        omer_doc = {"id": page['id'], "string_sequence": page['string_sequence'],
                    "text": page['text'],
                    "label_sequence": page['label_sequence'], "label": page['label'], 'QA': {},
                    'entity_article': ''}
        # article_extractor.extract(page['text'], page['label'])
        qas = defaultdict(list)
        for prop in page['facts']:
            relation = page['properties'][prop]
            omer_doc['QA'][prop] = []

            for fact in page['facts'][prop]:
                answer = fact['value']

                # match_index = distant_supervision(answer_sequence, omer_doc['label_sequence'],
                #                                   omer_doc['string_sequence'], omer_doc['sentence_breaks'])
                #
                # if not match_index:
                #     continue
                # start, end = match_index
                # sentence_sequence = omer_doc["string_sequence"][start:end]
                # sentence = rebuild_sentence(start, end, omer_doc['string_sequence'], omer_doc['break_levels'])
                # sentence = clean_sentence(sentence)

                sentence = string_distant_supervision(answer, omer_doc['label'], sentences)

                if not sentence:
                    continue

                qa = {"relation": relation['label'], "sentence": sentence,
                      "answer": fact['value'], "id": get_id_for_qa(page['id'], prop, fact['id']),
                      "answer_id": fact['id'], "prop_id": prop,
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


def read_questions_templates(path):
    templates = defaultdict(set)
    with open(path, "rt", encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter=",")
        for pid, relation, eng, google, template in reader:
            if template.strip():
                templates[pid].add(template.strip())
    return templates


def extract_examples(example_type):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.OMERMERGE_COLLECTION]
    documents = wikipedia.find({}, {"QA": 1, "id": 1, "label": 1, "entity_article": 1, "_id": 0})

    omer_props = set()
    with open("C:\\Users\sasce\PycharmProjects\WikiReading\src\\resources\omer_prop_id.txt", "rt",
              encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for pid, _ in reader:
            omer_props.add(pid)

    template_filler = TemplateFillerFactory.make_filler(config.LANG)

    question_templates = read_questions_templates(
        "resources/templates/templates_translation_{}.csv".format(config.LANG))

    with open("{}_qa_{}.json".format(config.LANG, example_type), "wt", encoding="utf8", newline="") as outf:
        i = 0
        for document in documents:
            for prop in document['QA']:
                if prop not in omer_props:
                    continue
                for qa in document['QA'][prop]:
                    if example_type and qa['example'] == example_type:
                        for template in question_templates[prop]:
                            question = template_filler.fill(template, document['label'],
                                                            article=document['entity_article'])
                            example = {'context': qa['sentence'], 'id': qa['id'], 'prop_id': qa['prop_id'],
                                       'property': qa['relation'], 'template': template, 'entity': document['label'],
                                       'answer': qa['answer'], 'question': question, 'entity_id': document['id']}
                            if example_type == "positive":
                                try:
                                    context = example['context']
                                    answer_text = example['answer']
                                    start_index = context.index(answer_text)
                                    end_index = start_index + len(answer_text)
                                    assert end_index <= len(context)
                                except:
                                    traceback.print_exc()
                                    i += 1
                                    print("Answer: {} ---- Context: {}".format(example['answer'], example['context']))
                                    continue
                                example['start_index'] = start_index
                                example['end_index'] = end_index
                                example['na'] = 1
                            else:
                                example['start_index'] = -1
                                example['end_index'] = -1
                                example['na'] = 0
                            outf.write(json.dumps(example, ensure_ascii=False) + "\n")
        print("Skipperd {} question/answers".format(i))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))
    build_omer({})
    # extract_examples("positive")
    # extract_examples("negative")
    logging.info("Completed %s", " ".join(sys.argv))
