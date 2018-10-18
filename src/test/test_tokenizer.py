import unittest

from tokenizers import SpacyTokenizer


class TestSpacyTokenizer(unittest.TestCase):

    def test_tokenizer_en(self):
        tokenizer = SpacyTokenizer('en')
        text = """Oecomys is a genus of rodent within the tribe Oryzomyini of family Cricetidae. It contains about 17 species, which live in trees and are distributed across forested parts of South America, extending into Panama and Trinidad.\n\nCarleton"""
        tokens, break_levels, _ = tokenizer.tokenize(text)
        gt_tokens = ["Oecomys", "is", "a", "genus", "of", "rodent", "within", "the", "tribe", "Oryzomyini", "of",
                     "family", "Cricetidae", ".", "It", "contains", "about", "17", "species", ",", "which", "live",
                     "in", "trees", "and", "are", "distributed", "across", "forested", "parts", "of", "South",
                     "America", ",", "extending", "into", "Panama", "and", "Trinidad", ".", "Carleton"]
        self.assertListEqual(tokens, gt_tokens)
        gt_breaks = [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 3, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                     0, 1, 1, 1, 1, 1, 0, 4]
        self.assertListEqual(break_levels, gt_breaks)
