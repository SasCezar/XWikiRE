import unittest

from tokenizers import SpacyTokenizer, KannadaTokenizer


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

SEP_MAPPING = {
    0: '',
    1: ' ',
    2: '\n',
    3: ' ',
    4: '\n\n'
}

def rebuild_sentence(start, end, tokens, breaks):
    sentence = ""
    for separator, token in zip(breaks[start:end], tokens[start:end]):
        sentence += SEP_MAPPING[separator] + token

    return sentence.strip()


class TestKannadaTokenizer(unittest.TestCase):

    def test_kannada(self):
        tokenizer = KannadaTokenizer()
        text = "ರಾಜ್ಯೋತ್ಸವ ಪ್ರಶಸ್ತಿ\nಕರ್ನಾಟಕ ರಾಜ್ಯ ಪ್ರಶಸ್ತಿಗಳು.\n\nರ್ನಾಟಕ ರಾಜ್ಯ ಪ್ರಶಸ್ತಿಗಳು"
        tokens, break_levels, _ = tokenizer.tokenize(text)
        rebuilt = rebuild_sentence(0, len(tokens), tokens, break_levels)
        self.assertEqual(rebuilt, text)
        self.assertEqual(len(tokens), len(break_levels))