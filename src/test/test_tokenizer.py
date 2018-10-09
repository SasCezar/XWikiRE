import unittest

from tokenizers import SpacyTokenizer

REV_TOKENS = {
    0: "",
    1: " ",
    2: "\n",
    3: ";",
    4: "\n\n"
}


def _rebuild(break_levels, tokens):
    final = ""
    for brk, tkn in zip(break_levels, tokens):
        final += REV_TOKENS[brk]
        final += tkn
    return final


class TestSpacyTokenizer(unittest.TestCase):

    def test_millennium(self):
        tokenizer = SpacyTokenizer('en')
        text = """This is a \n\n test! Let's see if it\nwords. Maybe not? Maybe yes!"""
        tokens, _, break_levels = tokenizer.tokenize(text)
        rebuilt_text = _rebuild(break_levels, tokens)
        self.assertEquals(text, rebuilt_text)
