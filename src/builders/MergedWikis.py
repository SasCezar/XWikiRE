import collections
import re
import traceback
from typing import Dict, List

from builders.builder import Builder
from utils.date_formatter import DateFormatterFactory

NO_UNIT = {'label': '', 'id': ''}

STOP_SECTIONS = {
    'en': ['See also', 'Notes', 'Further reading', 'External links'],
    'fr': ['Notes et références', 'Bibliographie', 'Voir aussi', 'Annexes', 'Références'],
    'it': ['Note', 'Bibliografia', 'Voci correlate', 'Altri progetti', 'Collegamenti esterni'],
    'de': ['Literatur', 'Siehe auch', 'Weblinks', 'Anmerkungen', 'Einzelnachweise und Anmerkungen',
           'Referenzen'],
    'es': ['Véase también', 'Notas', 'Referencias', 'Bibliografía', 'Enlaces externos', 'Notas y referencias']
}

DOC_CLEAN_KEYS = ['type', 'datatype', 'descriptions', 'claims', 'labels', 'sitelinks']


class MergedWikisBuilder(Builder):
    def __init__(self, ip, port, db, wikipedia, wikidata, destination, lang, locale):
        super().__init__(ip, port, db, wikipedia, destination)
        self._wikidata = db[wikidata]
        self._prop_cache = {}
        self._lang = lang
        self._date_formatter = DateFormatterFactory.get_formatter(lang, locale)
        self._stop_sections_re = re.compile("===?\s({})\s===?".format('|'.join(STOP_SECTIONS.get(lang, []))))

    def _build(self, doc, **kwargs):
        wikidata_doc = self._wikidata.find_one({"id": doc['wikidata_id']}, {"_id": 0})

        properties_ids = set(wikidata_doc['claims'].keys())
        uncached_prop_ids = list(properties_ids - set(self._prop_cache.keys()))
        prop_docs = list(self._wikidata.find({"id": {"$in": uncached_prop_ids}}, {"_id": 0}))
        uncached_prop = list(self._clean_wikidata_docs(prop_docs))
        list_prop = self._documents_to_dict(uncached_prop)
        self._prop_cache.update(list_prop)

        object_documents_ids = self._get_objects_id(wikidata_doc['claims'])
        object_documents = self._wikidata.find({"id": {"$in": object_documents_ids}}, {"_id": 0})
        documents_dict = self._documents_to_dict(self._clean_wikidata_docs(object_documents))

        facts = collections.defaultdict(list)
        for prop_id in wikidata_doc['claims']:
            for claim in wikidata_doc['claims'][prop_id]:
                try:
                    datatype = claim['mainsnak']['datavalue']['type']
                    if datatype == "string":
                        string_type = claim['mainsnak']['datatype']
                        if string_type in {'external-id', 'commonsMedia'}:
                            continue
                        value = claim['mainsnak']['datavalue']['value']
                        fact = self._create_string_fact(value)
                        facts[prop_id].append(fact)
                    elif datatype == "wikibase-entityid":
                        d_id = claim['mainsnak']['datavalue']['value']['id']
                        if d_id in documents_dict:
                            document = documents_dict[d_id]
                            fact = self._create_wikibase_fact(document)
                            facts[prop_id].append(fact)
                    elif datatype == "quantity":
                        d_id = claim['mainsnak']['datavalue']['value']['unit'].split("/")[-1]
                        amount = claim['mainsnak']['datavalue']['value']['amount']
                        unit = documents_dict[d_id] if d_id in documents_dict else NO_UNIT
                        fact = self._create_quantity_fact(amount, unit)
                        facts[prop_id].append(fact)
                    elif datatype == "time":
                        date = claim['mainsnak']['datavalue']['value']['time']
                        precision = claim['mainsnak']['datavalue']['value']['precision']
                        formatted_date = self._date_formatter.format(date, precision)
                        fact = self._create_time_fact(formatted_date, date)
                        facts[prop_id].append(fact)
                    else:
                        continue
                except:
                    traceback.print_exc()

        merged_document = self._clean_doc(wikidata_doc)
        merged_document['text'] = self._clean_text(doc['text'])
        merged_document['properties'] = {pid: self._prop_cache[pid] for pid in facts if pid in self._prop_cache}
        merged_document['facts'] = {pid: facts[pid] for pid in facts if pid in self._prop_cache}

        return merged_document

    def _clean_wikidata_docs(self, docs: List[Dict]) -> List[Dict]:
        """
        Cleans the documents in docs lists
        :param docs:
        :return:
        """
        for doc in docs:
            try:
                yield self._clean_doc(doc)
            except:
                continue

    DOC_CLEAN_KEYS = ['type', 'datatype', 'descriptions', 'claims', 'labels', 'sitelinks']

    def _clean_doc(self, doc: Dict) -> Dict:
        """
        Removes unwanted information from the document
        :param doc:
        :return:
        """
        doc['label'] = doc['labels'][self._lang]['value']
        aliases = doc['aliases'][self._lang] if self._lang in doc['aliases'] else []

        doc['aliases'] = [alias['value'] for alias in aliases]

        for key in DOC_CLEAN_KEYS:
            try:
                del doc[key]
            except KeyError:
                continue
        return doc

    def _get_objects_id(self, claims: Dict) -> List:
        """
        Given a list of claims returns a list of unique wikidata ids
        :param claims:
        :return:
        """
        ids = set()
        for prop in claims:
            for claim in claims[prop]:
                try:
                    datatype = claim['mainsnak']['datavalue']['type']
                    if datatype == "wikibase-entityid":
                        d_id = claim['mainsnak']['datavalue']['value']['id']
                        ids.add(d_id)
                    elif datatype == "quantity":
                        d_id = claim['mainsnak']['datavalue']['value']['unit'].split("/")[-1]
                        ids.add(d_id)
                    else:
                        continue
                except:
                    traceback.print_exc()

        return list(ids)

    def _documents_to_dict(self, documents: List[Dict]) -> Dict[str, Dict]:
        """
        Given a list of documents, returns a dict containing the ids of the documents a keys, the dict as value
        :param documents:
        :return:
        """
        res = {}
        for doc in documents:
            res[doc['id']] = doc
        return res

    @staticmethod
    def _create_string_fact(value: str) -> Dict:
        value = value.strip()
        fact = {"value": value, "type": "value", "id": value}
        return fact

    @staticmethod
    def _create_wikibase_fact(document: Dict) -> Dict:
        fact = {'value': document['label'], "type": "wikibase"}
        fact.update(document)
        return fact

    @staticmethod
    def _create_quantity_fact(amount: str, unit: Dict) -> Dict:
        amount = amount[1:] if amount.startswith("+") else amount
        value = amount + " " + unit['label']
        fid = amount + unit['id']
        fact = {"value": value.strip(), "type": "quantity", "id": fid}
        fact.update(unit)
        return fact

    @staticmethod
    def _create_time_fact(formatted_date: str, date: str):
        fact = {"value": formatted_date, "type": "date", "id": date}
        return fact

    def _clean_text(self, text: str) -> str:
        match = self._stop_sections_re.search(text)
        if match and match.start() > 0:
            text = text[:match.start()].strip()
        text = re.sub("===?\s[^=]+\s===?\n?", "", text)
        text = re.sub("\[\d+\]", "", text)
        text = re.sub("\n{3,}", "\n\n", text)
        return text
