import re
from abc import ABC, abstractmethod


class ArticleExtractorI(ABC):
    def __init__(self):
        self._articles = []
        self._re_template = ''

    @abstractmethod
    def extract(self, text, entity):
        raise NotImplemented


class ItalianArticleExtractor(ArticleExtractorI):
    def __init__(self):
        super().__init__()
        self._articles = ['Il', 'Lo', 'La', 'I', 'Gli', 'Le', 'L']
        self._re_template = "(?P<article>" + "|".join(
            ["\\b" + article + "\\b" for article in self._articles]) + ")(\s?|\'){}"

    def extract(self, text, entity):
        article = ''
        pattern = self._re_template.format("")
        match = re.search("^" + pattern, entity, re.IGNORECASE)
        if match:
            article = match.group('article')
            return article

        pattern = self._re_template.format(re.escape(entity.split()[0]))
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            article = match.group('article')
            return article

        return article

class ArticleExtractorFactory(object):
    @staticmethod
    def make_extractor(lang):
        if lang == 'it':
            return ItalianArticleExtractor()
