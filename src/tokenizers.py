from typing import List, Tuple

import spacy
from abc import ABC


class TokenizerI(ABC):

    def __init__(self):
        self.BREAK_LEVEL_TOKENS = {
            " ": [1],
            "\n": [2],
            "SENTENCE_BREAK": [3],
            "\n\n": [4],
            "\n\n ": [4, 1]
        }

        self.SENTENCE_BREAKS = ['.', '!', '?', '…', ';', ':', '...']

    def tokenize(self, text):
        raise NotImplemented

    def extract_break_levels(self, tokens):
        raise NotImplemented


class SpacyTokenizer(TokenizerI):
    def __init__(self, lang, **kwargs):
        super().__init__()
        self._tokenizer = spacy.load(lang, **kwargs)

    def tokenize(self, text: str) -> Tuple[List[str], List[int], List[str]]:
        doc = self._tokenizer(text)
        tokens, break_levels, pos_tagger_seq = self._decompose(doc)
        return tokens, break_levels, pos_tagger_seq

    def _decompose(self, doc):
        filtered_tokens = []
        pos_tagger_seq = []
        token_separators = [0]
        tokens = list(doc)
        for i, token in enumerate(tokens):
            if token.text not in self.BREAK_LEVEL_TOKENS:
                filtered_tokens.append(token.text)
                if token.text in self.SENTENCE_BREAKS:
                    token_separators += self.BREAK_LEVEL_TOKENS['SENTENCE_BREAK']
                    pos_tagger_seq.append(token.text + "\n")
                else:
                    pos_tagger_seq.append(token.text)
                if token.whitespace_:  # filter out empty strings
                    token_separators += self.BREAK_LEVEL_TOKENS[token.whitespace_]
                elif i + 1 < len(tokens) and str(tokens[i+1]) not in self.BREAK_LEVEL_TOKENS:
                    token_separators.append(0)
            else:
                token_separators += self.BREAK_LEVEL_TOKENS[token.text]
        return filtered_tokens, token_separators, pos_tagger_seq