import hashlib
import itertools
import re
import time
from abc import ABC, abstractmethod
from collections import Counter, defaultdict

import nltk
from pymongo import MongoClient

from article_extractors import ArticleExtractorFactory
from sling_tokenizer import SlingTokenizer


class Builder(ABC):
    def __init__(self, ip, port, db, source, destination, batch_size=500):
        self._client = MongoClient(ip, port)
        self._db = self._client[db]
        self._source = self._db[source]
        self._destination = self._db[destination]
        self._batch_size = batch_size

    def build(self, key, limit, **kwargs):
        n = 0
        processed = []
        mask = kwargs['mask'] if 'mask' in kwargs else {"_id": 0}
        start_time = time.time()
        counter = Counter()
        for doc in self._source.find({key: {"$gte": limit[0], "$lte": limit[1]}}, mask):
            result = self._build(doc)
            if result:
                document = result['document']
                counter.update(result['stats'])
                processed.append(document)
                n += 1

            if len(processed) >= self._batch_size:
                self._destination.insert_many(processed, ordered=False, bypass_document_validation=True)
                n += len(processed)
                processed = []

        if processed:
            self._destination.insert_many(processed, ordered=False, bypass_document_validation=True)
            n += len(processed)
        elapsed = int(time.time() - start_time)
        res = {"processed": n, "elapsed": elapsed}
        res.update(counter)

        return res

    @abstractmethod
    def _build(self, doc, **kwargs):
        return doc


class SRLBuilder(Builder):

    def __init__(self, ip, port, db, source, destination, language):
        super().__init__(ip, port, db, source, destination)
        self._tokenizer = nltk.sent_tokenize
        self._language = language

    def _build(self, doc, **kwargs):
        text = doc['text'].strip()
        if not text:
            return {}

        sentences = self._tokenizer(text.replace("\n\n", "\n"), language=self._language)
        srl = {"id": doc['id'], "text": doc['text'], "label": doc['label'], 'sentences': []}

        extracted = 0
        for prop in doc['facts']:
            relation = doc['properties'][prop]
            prop_labels = relation.get('aliases', [])
            prop_labels.append(relation['label'])
            print(prop_labels)
            for fact in doc['facts'][prop]:
                answer = fact['value']

                sentence, sentence_relation = self._distant_supervision(answer, srl['label'], prop_labels, sentences)

                if not sentence:
                    continue

                labeled_sentence = {"relation": relation['label'], "sentence": sentence, "answer": fact['value'],
                                    "id": self._get_id_for_qa(doc['id'], prop, fact['id']), "answer_id": fact['id'],
                                    "prop_id": prop, "sentence_relation": sentence_relation, "type": fact['type']}

                extracted += 1

                srl['sentences'].append(labeled_sentence)

        return {"document": srl, "stats": {"extracted": extracted}}

    @staticmethod
    def _distant_supervision(answer, entity, relations, sentences):
        e_template = "\\b" + re.escape(entity) + "\\b"
        a_template = "\\b" + re.escape(answer) + "\\b"
        r_template = "(?P<relation>" + "|".join(["\\b" + re.escape(relation) + "\\b" for relation in relations]) + ")"
        for sentence in sentences:
            relation = re.search(r_template, sentence)
            if re.search(e_template, sentence) and re.search(a_template, sentence) and relation:
                return sentence, relation.group("relation")

        return False

    @staticmethod
    def _get_id_for_qa(page_id, prop_id, answer_id):
        unique_str = " ".join([page_id, prop_id, answer_id])
        return hashlib.sha1(unique_str.encode("utf-8")).hexdigest()


class OmerBuilder(Builder):
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
        omer_doc = {"id": doc['id'], "text": doc['text'], "label": doc['label'], 'QA': {},
                    'entity_article': self._article_extractor.extract(doc['text'], doc['label'])}

        qas = defaultdict(list)
        for prop in doc['facts']:
            relation = doc['properties'][prop]
            omer_doc['QA'][prop] = []

            for fact in doc['facts'][prop]:
                answer = fact['value']

                sentence = self._distant_supervision(answer, omer_doc['label'], sentences)

                if not sentence:
                    continue

                qa = {"relation": relation['label'], "sentence": sentence,
                      "answer": fact['value'], "id": self._get_id_for_qa(doc['id'], prop, fact['id']),
                      "answer_id": fact['id'], "prop_id": prop,
                      "type": fact['type'], "example": "positive"}

                pos_count += 1

                qas[fact['type']].append(qa)

                omer_doc['QA'][prop].append(qa)

        negative_examples = self._create_negatives(qas)

        neg_count += len(negative_examples)
        for example in negative_examples:
            prop = example['prop_id']
            omer_doc['QA'][prop].append(example)

        return {"document": omer_doc, "stats": {"neg_count": neg_count, "pos_count": pos_count}}

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


class WikiReadingBuilder(Builder):
    def __init__(self, ip, port, db, source, destination):
        super().__init__(ip, port, db, source, destination)
        self._tokenizer = SlingTokenizer()
        self._pos_tagger = nltk.pos_tag

    def _build(self, doc, **kwargs):
        text = doc['text'].strip()
        if not text:
            return {}

        self._tokenize(doc)
        wikireading_doc = {'answer_breaks': [], 'answer_ids': ['IDs'], 'answer_location': [],
                           'answer_sequence': ['IDs'], 'answer_string_sequence': [], 'break_levels': [],
                           'document_sequence': ['IDs'], 'full_match_answer_location': [],
                           'paragraph_breaks': doc['paragraph_breaks'], 'question_sequence': ['IDs'],
                           'question_string_sequence': [],
                           'raw_answer_ids': ['IDs'], 'raw_answers': '', 'sentence_breaks': doc['sentence_breaks'],
                           'string_sequence': doc['string_sequence'], 'type_sequence': ['IDs']}

        for prop in doc['facts']:
            question = doc['properties'][prop]
            answer_string_sequence = []
            answer_breaks = []
            raw_answers = []
            full_match_answer_location = []
            answer_location = []
            for fact in doc['facts'][prop]:
                if answer_string_sequence:
                    answer_breaks.append(len(answer_string_sequence))
                raw_answers.append(fact['value'])
                answer = fact['value_sequence']
                answer_string_sequence += answer
                full_match_answer_location += self.find_full_matches(wikireading_doc["string_sequence"], answer)
                answer_location += self.find_matches(wikireading_doc["string_sequence"], answer)

            wikireading_doc['answer_breaks'] = answer_breaks
            wikireading_doc['answer_location'] = answer_location
            wikireading_doc['answer_string_sequence'] = answer_string_sequence
            wikireading_doc['full_match_answer_location'] = full_match_answer_location
            wikireading_doc['question_string_sequence'] = question['label_sequence']
            return wikireading_doc

    def _tokenize(self, document):
        article_text = document['text'].strip()
        tokens, break_levels, _ = self._tokenizer.tokenize(article_text)
        document['string_sequence'] = tokens
        document['break_levels'] = break_levels
        document['sentence_breaks'] = [i for i, brk in enumerate(break_levels) if brk >= 3]
        document['paragraph_breaks'] = [i for i, brk in enumerate(break_levels) if brk == 4]

        assert len(tokens) == len(break_levels)

        tokens, _, _ = self._tokenizer.tokenize(document['label'])
        document['label_sequence'] = tokens

        for prop in document['properties']:
            tokens, _, _ = self._tokenizer.tokenize(document['properties'][prop]['label'])
            document['properties'][prop]['label_sequence'] = tokens

        for prop in document['facts']:
            for fact in document['facts'][prop]:
                tokens, _, _ = self._tokenizer.tokenize(fact['value'])
                if len(tokens) == 0:
                    tokens = fact['value']
                fact['value_sequence'] = tokens

    @staticmethod
    def find_matches(sequence, answer):
        elements = set(answer)
        return [index for index, value in enumerate(sequence) if value in elements]

    def find_full_matches(self, list, sublist):
        results = []
        sll = len(sublist)
        for ind in (i for i, e in enumerate(list) if e == sublist[0]):
            if list[ind:ind + sll] == sublist:
                results.append(range(ind, ind + sll))

        return results

    @staticmethod
    def is_sublist(sublist, list):
        sll = len(sublist)
        try:
            for ind in (i for i, e in enumerate(list) if e == sublist[0]):
                if list[ind:ind + sll] == sublist:
                    return True
        except IndexError:
            print(sublist)
            print(list)
            raise
        return False
