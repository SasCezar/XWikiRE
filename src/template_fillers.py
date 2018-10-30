import re
from abc import ABC


class TemplateFillerI(ABC):
    def fill(self, template: str, entity: str, **kwargs):
        return template.replace("XXX", entity)


class ItalianTemplateFiller(TemplateFillerI):
    def __init__(self):
        self._reduction_rules = {'diil': 'del', 'dilo': 'dello', 'dila': 'della',
                                 'dii': 'dei', 'digli': 'degli', 'dile': 'delle', 'dil\'': 'dell\'',
                                 'suil': 'sul', 'sulo': 'sullo', 'sula': 'sulla', 'sui': 'sui',
                                 'sugli': 'sugli', 'sule': 'sulle'}

        self._template = "(?P<preposition>" + "|".join(["\\b" + preposition + "\\s"
                                                        for preposition in self._reduction_rules.keys()]) + ")"
        self._finder = re.compile(self._template, re.IGNORECASE | re.MULTILINE)

    def fill(self, template: str, entity: str, **kwargs):
        article = kwargs['article'].lower()
        if entity.lower().startswith(article):
            entity = re.sub("\\b" + article + "\s", "", entity, 1, re.IGNORECASE)

        template = template.replace("XXX", entity)

        if article:
            template = template.replace("YYY", article)
            template = self._reduce(template)
            if '\' ' + entity in template:
                template = template.replace("\' ", "\'")

        template = re.sub("\s{2,}", " ", template)
        return template

    def _reduce(self, template):
        match = self._finder.search(template)
        if match:
            preposition = match.group('preposition').lower().strip()
            template = template.replace(preposition, self._reduction_rules[preposition])

        return template


class TemplateFillerFactory(object):
    @staticmethod
    def make_filler(lang):
        if lang == "en":
            return TemplateFillerI()
        if lang == "it":
            return ItalianTemplateFiller()
