import unittest

from template_fillers import ItalianTemplateFiller


class TestItalianTemplateFiller(unittest.TestCase):
    def test_gli(self):
        filler = ItalianTemplateFiller()

        template = filler.fill("Chi è il presidente diYYY XXX?", "Stati Uniti", article="Gli")
        self.assertEqual("Chi è il presidente degli Stati Uniti?", template)

    def test_l(self):
        filler = ItalianTemplateFiller()

        template = filler.fill("Chi è il presidente diYYY XXX?", "America", article="L'")
        self.assertEqual("Chi è il presidente dell'America?", template)

    def test_la_in(self):
        filler = ItalianTemplateFiller()

        template = filler.fill("Chi è l'autore diYYY XXX?", "La bella e la bestia", article="La")
        self.assertEqual("Chi è l'autore della bella e la bestia?", template)
