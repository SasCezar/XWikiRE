import csv
import json
import logging
import multiprocessing
import time
import traceback
from collections import defaultdict

from natural.date import compress
from pymongo import MongoClient

import config
from builders.QA import QABuilder
from template_fillers import TemplateFillerFactory
from utils import get_chunks


def read_questions_templates(path):
    templates = defaultdict(set)
    with open(path, "rt", encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter=",")
        for pid, relation, eng, google, template in reader:
            if template.strip():
                templates[pid.strip()].add(template.strip())
    return templates


def extract_examples(example_type):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.QAMERGE_COLLECTION]
    documents = wikipedia.find({}, {"QA": 1, "id": 1, "label": 1, "entity_article": 1, "_id": 0})

    omer_props = set()
    with open("../resources/levy_prop_id.txt", "rt",
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
                                    logging.info(
                                        "Answer: {} ---- Context: {}".format(example['answer'], example['context']))
                                    continue
                                example['start_index'] = start_index
                                example['end_index'] = end_index
                                example['na'] = 1
                            else:
                                example['start_index'] = -1
                                example['end_index'] = -1
                                example['na'] = 0
                            outf.write(json.dumps(example, ensure_ascii=False) + "\n")
        logging.info("Skipped {} question/answers".format(i))


def build_qa(limit):
    builder = QABuilder(config.MONGO_IP, config.MONGO_PORT, config.DB, config.WIKIMERGE_COLLECTION,
                        config.QAMERGE_COLLECTION, config.LANGUAGE)
    return builder.build("id", limit)


def run_parallel():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikidocs = list(wikipedia.find({}, {'wikidata_id': 1, '_id': 0}).sort('wikidata_id'))
    chunks = get_chunks(wikidocs, config.CHUNK_SIZE, 'wikidata_id')
    del wikidocs
    start_time = time.time()
    total = 0

    pool = multiprocessing.Pool(config.NUM_WORKERS)
    for res in pool.imap(build_qa, chunks):
        total += res['processed']
        res['total'] = total
        part = int(time.time() - start_time)
        res['elapsed'] = compress(res['elapsed'])
        res['total_elapsed'] = compress(part)
        logging.info("Processed {processed} ({total} in total) documents in {elapsed} (running time {"
                     "total_elapsed})".format(**res))

    pool.terminate()

    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {}".format(total, compress(elapsed)))
    return


if __name__ == '__main__':
    run_parallel()
    extract_examples("positive")
    extract_examples("negaive")