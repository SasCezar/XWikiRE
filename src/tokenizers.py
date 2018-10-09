from typing import List, Tuple

import spacy
from abc import ABC


class TokenizerI(ABC):

    def __init__(self):
        self.BREAK_LEVEL_TOKENS = {
            " ": 1,
            "\n": 2,
            "SENTENCE_BREAK": 3,
            "\n\n": 4,
        }

        self.SENTENCE_BREAKS = ['.', '!', '?', '…', ';', ':', '...']

    def tokenize(self, text):
        raise NotImplemented

    def extract_break_levels(self, tokens):
        breaks = []
        tokens_iter = enumerate(tokens)
        for i, token in tokens_iter:
            if token in self.BREAK_LEVEL_TOKENS:
                breaks.append(self.BREAK_LEVEL_TOKENS[token])
            elif token in self.SENTENCE_BREAKS:
                breaks.append(self.BREAK_LEVEL_TOKENS['SENTENCE_BREAK'])
            else:
                breaks.append(0)
        return breaks


class SpacyTokenizer(TokenizerI):
    def __init__(self, lang, **kwargs):
        super().__init__()
        self._tokenizer = spacy.load(lang, **kwargs)

    def tokenize(self, text: str) -> Tuple[List[str], List[int]]:
        doc = self._tokenizer(text)
        tokens, break_levels = self._decompose(doc)
        return tokens, break_levels

    def _decompose(self, doc):
        tokenized_text = []
        filtered_tokens = []
        token_separators = [0]
        for token in doc:
            tokenized_text.append(token.text) if token.text not in self.BREAK_LEVEL_TOKENS else None
            filtered_tokens.append(token.text)
            if token.whitespace_:  # filter out empty strings
                tokenized_text.append(token.whitespace_)
                token_separators.append(self.BREAK_LEVEL_TOKENS[token.whitespace_])
            else:
                token_separators.append(0)
        return filtered_tokens, token_separators