import nltk

from builders.builder import Builder


class WikiReadingBuilder(Builder):
    def __init__(self, ip, port, db, source, destination):
        super().__init__(ip, port, db, source, destination)
        self._tokenizer = lambda x: x.split()  # SlingTokenizer() #ToDo Fix tokenizer
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