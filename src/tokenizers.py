from abc import ABC
from typing import List, Tuple

import spacy
from polyglot.text import Text

SENTENCE_BREAKS = {'.', '!', '?', '…', '...'}


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
        break_levels = self._get_break_levels(doc)
        pos_tagger_seq = []
        filtered_tokens = [token.text.strip() for token in doc if
                           token.text.strip(" ") not in self.BREAK_LEVEL_TOKENS if token.text.strip()]
        for token in filtered_tokens:
            element = token + "\n" if token in self.SENTENCE_BREAKS else token
            pos_tagger_seq.append(element)
        # if len(filtered_tokens) != len(break_levels):
        #    print(repr(text))
        # assert len(filtered_tokens) == len(break_levels)
        return filtered_tokens, break_levels, pos_tagger_seq

    def _get_break_levels(self, tokens):
        token_separators = [0]
        i = 0
        for prev, curr, next in zip(tokens, tokens[1:], tokens[2:]):
            i += 1
            if curr.text.strip(" ") in self.BREAK_LEVEL_TOKENS:
                token_separators.append(self.BREAK_LEVEL_TOKENS[curr.text.strip(" ")])
                continue
            if curr.text.strip(" ") in self.SENTENCE_BREAKS:
                separator = 1 if prev.whitespace_ else 0
                token_separators.append(separator) if prev.text.strip(" ") not in self.BREAK_LEVEL_TOKENS else None
                if next.text.strip(" ") not in self.BREAK_LEVEL_TOKENS and curr.whitespace_:
                    token_separators.append(self.BREAK_LEVEL_TOKENS['SENTENCE_BREAK'])
                elif next.text.strip(" ") not in self.BREAK_LEVEL_TOKENS:
                    token_separators.append(0)
                continue
            if prev.text.strip(" ") not in self.SENTENCE_BREAKS \
                    and prev.text.strip(" ") not in self.BREAK_LEVEL_TOKENS:
                separator = 1 if prev.whitespace_ else 0
                token_separators.append(separator)

            if len(tokens) == i + 2:
                if not curr.whitespace_:
                    token_separators.append(0)
                else:
                    token_separators.append(1)

        return token_separators


class KannadaTokenizer(TokenizerI):
    def __init__(self):
        super().__init__()
        self._tokenizer = Text

    def tokenize(self, text):
        tokens = self._tokenizer(text).words
        token_span = align_tokens(tokens, text)
        spans = list(sum(token_span, ()))
        all_tokens = [text[b: e] for b, e in zip(spans, spans[1:])]
        break_levels = self._get_break_levels(all_tokens)

        pos_tagger_seq = []
        for token in tokens:
            element = token + "\n" if token in self.SENTENCE_BREAKS else token
            pos_tagger_seq.append(element)

        return tokens, break_levels, pos_tagger_seq

    def _get_break_levels(self, tokens):
        token_separators = [0]
        for prev, curr, next in zip(tokens, tokens[1:], tokens[2:]):
            if not curr:
                continue
            if curr in self.BREAK_LEVEL_TOKENS:
                token_separators.append(self.BREAK_LEVEL_TOKENS[curr])
                continue
            if curr in self.SENTENCE_BREAKS:
                separator = 1 if prev == ' ' else 0
                token_separators.append(separator)
                if next not in self.BREAK_LEVEL_TOKENS:
                    token_separators.append(self.BREAK_LEVEL_TOKENS['SENTENCE_BREAK'])
                continue
            if prev not in self.SENTENCE_BREAKS \
                    and prev not in self.BREAK_LEVEL_TOKENS \
                    and curr not in self.SENTENCE_BREAKS:
                separator = 1 if prev == ' ' else 0
                token_separators.append(separator)

        return token_separators


def align_tokens(tokens, sentence):
    """
    This module attempt to find the offsets of the tokens in *s*, as a sequence
    of ``(start, end)`` tuples, given the tokens and also the source string.

        >>> from nltk.tokenize import TreebankWordTokenizer
        >>> from nltk.tokenize.util import align_tokens
        >>> s = str("The plane, bound for St Petersburg, crashed in Egypt's "
        ... "Sinai desert just 23 minutes after take-off from Sharm el-Sheikh "
        ... "on Saturday.")
        >>> tokens = TreebankWordTokenizer().tokenize(s)
        >>> expected = [(0, 3), (4, 9), (9, 10), (11, 16), (17, 20), (21, 23),
        ... (24, 34), (34, 35), (36, 43), (44, 46), (47, 52), (52, 54),
        ... (55, 60), (61, 67), (68, 72), (73, 75), (76, 83), (84, 89),
        ... (90, 98), (99, 103), (104, 109), (110, 119), (120, 122),
        ... (123, 131), (131, 132)]
        >>> output = list(align_tokens(tokens, s))
        >>> len(tokens) == len(expected) == len(output)  # Check that length of tokens and tuples are the same.
        True
        >>> expected == list(align_tokens(tokens, s))  # Check that the output is as expected.
        True
        >>> tokens == [s[start:end] for start, end in output]  # Check that the slices of the string corresponds to the tokens.
        True

    :param tokens: The list of strings that are the result of tokenization
    :type tokens: list(str)
    :param sentence: The original string
    :type sentence: str
    :rtype: list(tuple(int,int))
    """
    point = 0
    offsets = []
    for token in tokens:
        try:
            start = sentence.index(token, point)
        except ValueError:
            raise ValueError('substring "{}" not found in "{}"'.format(token, sentence))
        point = start + len(token)
        offsets.append((start, point))
    return offsets
