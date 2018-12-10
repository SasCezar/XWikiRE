from polyglot.text import Text

from tokenizers.TokenizerI import TokenizerI
from tokenizers.utils import align_tokens


class PolyglotTokenizer(TokenizerI):
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

    # Bugged
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