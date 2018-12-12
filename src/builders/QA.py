import csv
import hashlib
import itertools
import json
import logging
import re
import traceback
from collections import defaultdict

import nltk
from pymongo import MongoClient

import config
from utils.article_extractors import ArticleExtractorFactory
from builders.builder import Builder
from utils.template_fillers import TemplateFillerFactory
from utils import load_props


class QABuilder(Builder):
    def __init__(self, ip, port, db, source, destination, language):
        super().__init__(ip, port, db, source, destination)
        self._language = language
        self._article_extractor = ArticleExtractorFactory.make_extractor(language)
        self._tokenizer = nltk.sent_tokenize

    def _build(self, doc, **kwargs):
        pos_count = 0
        neg_count = 0
        text = doc['text'].strip()
        if not text:
            return {}

        sentences = self._tokenizer(text.replace("\n\n", "\n"), language=self._language)
        qa_doc = {"id": doc['id'], "text": doc['text'], "label": doc['label'], 'QA': {},
                    'entity_article': self._article_extractor.extract(doc['text'], doc['label'])}

        qas = defaultdict(list)
        for prop in doc['facts']:
            relation = doc['properties'][prop]
            qa_doc['QA'][prop] = []

            for fact in doc['facts'][prop]:
                answer = fact['value']

                sentence = self._distant_supervision(answer, qa_doc['label'], sentences)

                if not sentence:
                    continue

                qa = {"relation": relation['label'], "sentence": sentence,
                      "answer": fact['value'], "id": self._get_id_for_qa(doc['id'], prop, fact['id']),
                      "answer_id": fact['id'], "prop_id": prop,
                      "type": fact['type'], "example": "positive"}

                pos_count += 1

                qas[fact['type']].append(qa)

                qa_doc['QA'][prop].append(qa)

        negative_examples = self._create_negatives(qas)

        neg_count += len(negative_examples)
        for example in negative_examples:
            prop = example['prop_id']
            qa_doc['QA'][prop].append(example)

        return {"document": qa_doc, "stats": {"neg_count": neg_count, "pos_count": pos_count}}

    @staticmethod
    def _get_id_for_qa(page_id, prop_id, answer_id):
        unique_str = " ".join([page_id, prop_id, answer_id])
        return hashlib.sha1(unique_str.encode("utf-8")).hexdigest()

    @staticmethod
    def _distant_supervision(answer, entity, sentences):
        e_template = "\\b" + re.escape(entity) + "\\b"
        a_template = "\\b" + re.escape(answer) + "\\b"
        for sentence in sentences:
            if re.search(e_template, sentence) and re.search(a_template, sentence):
                return sentence

        return False

    def _create_negatives(self, qas):
        neg_examples = []
        for type in qas:
            combinations = itertools.combinations(qas[type], 2)
            for a, b in combinations:
                negative = self._create_negative(a, b)
                if negative:
                    neg_examples.append(negative)

        return neg_examples

    def _create_negative(self, a, b):
        if a['prop_id'] == b['prop_id']:
            return {}
        e_template = "\\b" + re.escape(a['answer']) + "\\b"
        if not re.search(e_template, b['sentence']):
            neg_a = {"relation": a['relation'], "sentence": b['sentence'],
                     "answer": "", "id": self._get_id_for_qa(a['id'], a['prop_id'], b['id']),
                     "answer_id": 0, "prop_id": a['prop_id'],
                     "type": a['type'], "example": "negative", "source_a": a['id'], "source_b": b['id']}
            return neg_a
        return {}


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
    wikipedia = db[config.QA_COLLECTION]
    documents = wikipedia.find({}, {"QA": 1, "id": 1, "label": 1, "entity_article": 1, "_id": 0})

    levy_props = load_props()

    template_filler = TemplateFillerFactory.make_filler(config.LANG)

    question_templates = read_questions_templates(
        "resources/templates/templates_translation_{}.csv".format(config.LANG))

    with open("{}_qa_{}.json".format(config.LANG, example_type), "wt", encoding="utf8", newline="") as outf:
        i = 0
        for document in documents:
            for prop in document['QA']:
                if prop not in levy_props:
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
