import re
from abc import ABC


class TemplateFillerI(ABC):
    def fill(self, template: str, entity: str, **kwargs):
        return template.replace("XXX", entity)


class ItalianTemplateFiller(TemplateFillerI):
    def __init__(self):
        self._reduction_rules = {'diil': 'del', 'dilo': 'dello', 'dila': 'della', 'dii': 'dei', 'digli': 'degli',
                                 'dile': 'delle', 'dil': 'dell\'',
                                 'ail': 'al', 'alo': 'allo', 'ala': 'alla', 'ai': 'ai', 'agli': 'agli', 'ale': 'alle',
                                 'dail': 'dal', 'dalo': 'dallo', 'dala': 'dalla', 'dai': 'dai', 'dagli': 'dagli',
                                 'dale': 'dalle',
                                 'inil': 'nel', 'inlo': 'nello', 'inla': 'nella', 'ini': 'nei', 'ingli': 'negli',
                                 'inle': 'nelle',
                                 'conil': 'col', 'conlo': 'cóllo', 'conla': 'cólla', 'coni': 'coi', 'congli': 'cogli',
                                 'conle': 'cólle',
                                 'suil': 'sul', 'sulo': 'sullo', 'sula': 'sulla', 'sui': 'sui', 'sugli': 'sugli',
                                 'sule': 'sulle',
                                 'peril': 'pel', 'perlo': 'pello', 'perla': 'pella', 'peri': 'pei', 'pergli': 'pegli',
                                 'perle': 'pelle'}

        self._template = "(?P<preposition>" + "|".join(["\\b" + preposition + "\\b"
                                                        for preposition in self._reduction_rules.keys()]) + ")"
        self._finder = re.compile(self._template, re.IGNORECASE)
        self._articles_gender = {'il': 'o', 'lo': 'o', 'i': 'i', 'gli': 'i', 'la': 'a', 'le': 'e'}

    def fill(self, template: str, entity: str, **kwargs):
        article = kwargs['article'].lower()
        article_in_entity = True if entity.lower().startswith(article) else False

        if article:
            if article_in_entity and re.search("(di|a|da|in|con|su|per)YYY", template):
                entity = re.sub("\\b" + article + "\\b", "", entity, 1, re.IGNORECASE)
                template = template.replace("YYY", article)
            elif article_in_entity:
                template = template.replace("YYY", "")
            else:
                template = template.replace("YYY", article)
            template = self._reduce(template)
        else:
            template = template.replace("YYY", "")

        gender = self._articles_gender.get(article, 'o')
        template = template.replace("GGG", gender)
        template = template.replace("XXX", entity)
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


class FrenchTemplateFiller(TemplateFillerI):
    def __init__(self):
        self._reduction_rules = {'dele': 'du', 'dela': 'de la', 'del': 'de l\'', 'deles': 'des',
                                 'àle': 'au', 'àla': 'à la', 'àl': 'à l\'', 'àles': 'aux'}

        self._template = "(?P<preposition>" + "|".join(["\\b" + preposition + "\\b"
                                                        for preposition in self._reduction_rules.keys()]) + ")"
        self._finder = re.compile(self._template, re.IGNORECASE)


class GermanTemplateFiller(TemplateFillerI):
    def fill(self, template: str, entity: str, **kwargs):
        article = kwargs['article'].lower()

        article_in_entity = True if entity.lower().startswith(article) else False
        if article_in_entity:
            article = ""
        template = re.sub("YYY(a|d|g)", article, template)
        template = template.replace("XXX", entity)

        return template


class TemplateFillerFactory(object):
    @staticmethod
    def make_filler(lang):
        if lang == "en":
            return TemplateFillerI()
        if lang == "it":
            return ItalianTemplateFiller()
        if lang == "de":
            return GermanTemplateFiller()
