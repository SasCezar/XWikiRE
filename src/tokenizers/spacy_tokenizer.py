from typing import Tuple, List

import spacy

from tokenizers.TokenizerI import TokenizerI


class SpacyTokenizer(TokenizerI):
    def __init__(self, lang, **kwargs):
        super().__init__()
        self._tokenizer = spacy.load("xx", **kwargs)

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

    # Bugged
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