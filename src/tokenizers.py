import spacy
from abc import ABC


class TokenizerI(ABC):
    def tokenize(self, text):
        raise NotImplemented


class SpacyTokenizer(TokenizerI):
    def __init__(self, lang, **kwargs):
        self._tokenizer = spacy.load(lang, **kwargs)

    def tokenize(self, text, keep_separators=True):
        doc = self._tokenizer(text)
        tokens = self._get_tokens(doc, keep_separators)
        return tokens

    def _get_tokens(self, doc, keep_separators):
        tokenized_text = []
        for token in doc:
            tokenized_text.append(token.text)
            if token.whitespace_ and keep_separators:  # filter out empty strings
                tokenized_text.append(token.whitespace_)
        return tokenized_text
