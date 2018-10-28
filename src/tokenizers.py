from typing import List, Tuple

import spacy
from abc import ABC
from stanfordcorenlp import StanfordCoreNLP

SENTENCE_BREAKS = {'.', '!', '?', '…', ';', ':', '...'}


class TokenizerI(ABC):

    def __init__(self):
        self.BREAK_LEVEL_TOKENS = {
            " ": 1,
            "\n": 2,
            "SENTENCE_BREAK": 3,
            "\n\n": 4,
        }

        self.SENTENCE_BREAKS = SENTENCE_BREAKS

    def tokenize(self, text):
        raise NotImplemented


class SpacyTokenizer(TokenizerI):
    def __init__(self, lang, **kwargs):
        super().__init__()
        self._tokenizer = spacy.load(lang, **kwargs)

    def tokenize(self, text: str) -> Tuple[List[str], List[int], List[str]]:
        doc = self._tokenizer(text)
        filtered_tokens = [token.text for token in doc if token.text not in self.BREAK_LEVEL_TOKENS]
        break_levels = self._get_break_levels(doc)
        pos_tagger_seq = []
        for token in filtered_tokens:
            element = token + "\n" if token in self.SENTENCE_BREAKS else token
            pos_tagger_seq.append(element)

        return filtered_tokens, break_levels, pos_tagger_seq

    def _get_break_levels(self, tokens):
        token_separators = [0]
        for prev, curr, next in zip(tokens, tokens[1:], tokens[2:]):
            if curr.text in self.BREAK_LEVEL_TOKENS:
                token_separators.append(self.BREAK_LEVEL_TOKENS[curr.text])
                continue
            if curr.text in self.SENTENCE_BREAKS:
                separator = 1 if prev.whitespace_ else 0
                token_separators.append(separator)
                if next.text not in self.BREAK_LEVEL_TOKENS:
                    token_separators.append(self.BREAK_LEVEL_TOKENS['SENTENCE_BREAK'])
                continue
            if prev.text not in self.SENTENCE_BREAKS \
                    and prev.text not in self.BREAK_LEVEL_TOKENS \
                    and curr.text not in self.SENTENCE_BREAKS:
                separator = 1 if prev.whitespace_ else 0
                token_separators.append(separator)

        return token_separators


class StanfordTokenizer(TokenizerI):
    def __init__(self, host='http://localhost', port=9000):
        super().__init__()
        self.nlp = StanfordCoreNLP(host, port=port, timeout=30000)

    def tokenize(self, text):
        return self.nlp.word_tokenize(text)