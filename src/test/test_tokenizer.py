import unittest

from tokenizers import SpacyTokenizer

REV_TOKENS = {
    0: "",
    1: " ",
    2: "\n",
    4: "\n\n"
}


def _rebuild(break_levels, tokens):
    final = ""
    j = 0
    for i in range(len(tokens)):
        brk = break_levels[i+j]
        tkn = tokens[i]
        if brk != 3:
            final += REV_TOKENS[brk]
        else:
            j += 1
            final += REV_TOKENS[break_levels[i+j]]
        final += tkn
    return final


class TestSpacyTokenizer(unittest.TestCase):

    def test_tokenizer(self):
        tokenizer = SpacyTokenizer('en')
        text = """This is a\n\ntest! Let's see if it\nworks."""
        tokens, break_levels, _ = tokenizer.tokenize(text)
        rebuilt_text = _rebuild(break_levels, tokens)
        self.assertEquals(text, rebuilt_text)
        gt_tokens = ["This", "is", "a", "test", "!", "Let", "'s", "see", "if", "it", "works", "."]
        self.assertListEqual(tokens, gt_tokens)
        gt_breaks = [0, 1, 1, 4, 0, 3, 1, 0, 1, 1, 1, 2, 0, 3]
        self.assertListEqual(break_levels, gt_breaks)
