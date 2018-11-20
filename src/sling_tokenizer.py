import sling


class SlingTokenizer(object):
    def __init__(self, **kwargs):
        self.tokenizer = sling.tokenize

    def tokenize(self, text):
        doc = self.tokenizer(text.encode('utf8'))
        break_levels = [token.brk for token in doc.tokens]
        tokens = [token.word for token in doc.tokens]
        assert len(tokens) == len(break_levels)
        return tokens, break_levels, None
