import json
import logging
import re
from collections import defaultdict

import nltk
from ftfy import fix_text
from sacremoses import MosesTokenizer
from pymongo import MongoClient

from builders.builder import Builder


class SRLBuilder(Builder):

    def __init__(self, ip, port, db, source, destination, lang, language):
        super().__init__(ip, port, db, source, destination)
        self._sent_tokenizer = nltk.sent_tokenize
        self._word_tokenizer = MosesTokenizer(lang)
        self._pos_tagger = nltk.pos_tag
        self._language = language

    def _build(self, doc, **kwargs):
        text = doc['text'].strip()
        if not text:
            return {}

        sentences = self._sent_tokenizer(text.replace("\n\n", "\n"), language="english")
        srl = {"id": doc['id'], "text": doc['text'], "label": doc['label'],
               "label_sequence": self._tokenize(doc['label']),
               'sentences': defaultdict(lambda: {"sentence": "", "sentence_sequence": [], "relations": []})}

        extracted = 0
        skipped = 0
        seen = set()
        for prop in doc['facts']:
            relation = doc['properties'][prop]
            prop_labels = relation.get('aliases', [])
            prop_labels.append(relation['label'])
            for fact in doc['facts'][prop]:
                answer = fact['value']
                sentence, sentence_relation = self._distant_supervision(answer, srl['label'], prop_labels, sentences)

                if not sentence:
                    continue

                sentence_id = self._get_id(sentence)
                if sentence_id not in seen:
                    sentence_sequence = self._tokenize(sentence)
                    pos = self._pos_tagger(sentence_sequence, lang="eng")
                    srl['sentences'][sentence_id]['sentence'] = sentence
                    srl['sentences'][sentence_id]['sentence_sequence'] = sentence_sequence
                    srl['sentences'][sentence_id]['pos'] = [tag for _, tag in pos]
                    entity_location = self.find_full_matches(sentence_sequence, srl['label_sequence'])
                    if not entity_location:
                        logging.error(
                            "Unable to find {} in sequence {}".format(srl["label_sequence"], sentence_sequence))

                        skipped += 1
                        continue
                    extracted += 1
                    srl['sentences'][sentence_id]['full_match_entity_location'] = entity_location
                    seen.add(sentence_id)
                else:
                    sentence_sequence = srl['sentences'][sentence_id]['sentence_sequence']

                answer_sequence = self._tokenize(fact['value'])
                answer_location = self.find_full_matches(sentence_sequence, answer_sequence)
                if not answer_location:
                    logging.error("Unable to find {} in sequence {}".format(answer_sequence, sentence_sequence))
                    skipped += 1
                    continue

                relation_sequence = self._tokenize(sentence_relation)
                relation_location = self.find_full_matches(sentence_sequence, relation_sequence)
                if not relation_location:
                    logging.error("Unable to find {} in sequence {}".format(relation_sequence, sentence_sequence))
                    skipped += 1
                    continue

                triple = {"relation": relation['label'], "answer": fact['value'],
                          "answer_sequence": answer_sequence, 'answer_location': answer_location,
                          "id": self._get_id_for_qa(doc['id'], prop, fact['id']),
                          "answer_id": fact['id'], "prop_id": prop, "sentence_relation": sentence_relation,
                          "relation_sequence": relation_sequence, "relation_location": relation_location,
                          "type": fact['type']}

                srl['sentences'][sentence_id]['relations'].append(triple)
        if not len(srl['sentences']):
            return {}
        return {"document": srl, "stats": {"extracted": extracted, "skipped": skipped}}

    @staticmethod
    def _distant_supervision(answer, entity, relations, sentences):
        e_template = "\\b" + re.escape(entity) + "\\b"
        a_template = "\\b" + re.escape(answer) + "\\b"
        r_template = "(?P<relation>" + "|".join(["\\b" + re.escape(relation) + "\\b" for relation in relations]) + ")"
        for sentence in sentences:
            relation = re.search(r_template, sentence)
            if re.search(e_template, sentence) and re.search(a_template, sentence) and relation:
                return sentence, relation.group("relation")

        return False, False

    def _get_id_for_qa(self, page_id, prop_id, answer_id):
        unique_str = " ".join([page_id, prop_id, answer_id])
        return self._get_id(unique_str)

    def _tokenize(self, sentence):
        sentence = sentence.replace("\n", "")
        return [fix_text(t) for t in self._word_tokenizer.tokenize(sentence)]

    @staticmethod
    def find_full_matches(iterable, sublist):
        results = []
        sll = len(sublist)
        for ind in (i for i, e in enumerate(iterable) if e == sublist[0]):
            if iterable[ind:ind + sll] == sublist:
                results.append(list(range(ind, ind + sll)))

        return results


class SRLExporter(object):
    def __init__(self, ip, port, db, collection, language):
        self._client = MongoClient(ip, port)
        self._db = self._client[db]
        self._wiki_srl = self._db[collection]
        self._lang = language

    def export(self, out_path):
        documents = self._wiki_srl.find({}, {"_id": 0, "text": 0})
        i = 0
        nv_count = 0
        with open(out_path, "wt", encoding="utf8", newline="") as outf:
            for document in documents:
                for sentence_id in document['sentences']:
                    sentence = document['sentences'][sentence_id]
                    text = sentence['sentence']
                    pos = sentence['pos']
                    try:
                        entity_locations, breaks = self._get_locations(sentence['full_match_entity_location'])
                        if breaks:
                            raise Exception
                    except:
                        i += 1
                        continue

                    out_doc = {'lang': self._lang, 'id': sentence_id, 'sentence': text, 'entity': document['label'],
                               'sentence_sequence': sentence['sentence_sequence'], 'pos': pos,
                               'entity_id': document['id'],
                               'entity_sequence': document['label_sequence'], 'relations': [],
                               'entity_locations': entity_locations}

                    for relation in sentence['relations']:
                        if not self._relation_contains_verb(relation['relation_location'][0], pos):
                            nv_count += 1
                            continue
                        answer_locations, answer_locations_breaks = self._get_locations(relation['answer_location'])
                        relation_locations, relation_locations_breaks = self._get_locations(
                            relation['relation_location'])
                        if answer_locations_breaks or relation_locations_breaks:
                            continue
                        rel_doc = {'property_id': relation['prop_id'],
                                   'relation_sequence': relation['relation_sequence'],
                                   'property_label': relation['relation'], 'relation_locations': relation_locations,
                                   # 'relation_locations_breaks': relation_locations_breaks,
                                   'sentence_relation': relation['sentence_relation'],
                                   'answer_id': relation['answer_id'],
                                   'answer': relation['answer'], 'answer_locations': answer_locations,
                                   # 'answer_location_breaks': answer_locations_breaks,
                                   'answer_sequence': relation['answer_sequence'], 'relation_id': relation['id']}

                        out_doc['relations'].append(rel_doc)

                    if out_doc['relations']:
                        obj = json.dumps(out_doc, ensure_ascii=False)
                        outf.write(obj + "\n")

    @staticmethod
    def _relation_contains_verb(relation_position, pos):
        start = relation_position[0]
        end = relation_position[-1] + 1

        if any([True for tag in pos[start:end] if tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']]):
            return True
        else:
            return False

    @staticmethod
    def _get_locations(locations):
        pos = [x for k in locations for x in k]
        breaks = []
        if len(locations) > 1:
            i = 0
            for location in locations[:-1]:
                size = len(location)
                breaks.append(size + i)
                i += size

        return pos, breaks
