from utils.date_formatter import DateFormatterFactory
from tokenizers.spacy_tokenizer import SpacyTokenizer

LANG = 'fr'
LANGUAGE = 'french'
LOCALE = 'fr-FR'

NUM_WORKERS = 5
CHUNK_SIZE = 1000

MONGO_IP = "localhost"
MONGO_PORT = 27017
DB = "WikiReading"
WIKIDATA_COLLECTION = "wikidata"
WIKIPEDIA_COLLECTION = "{}wiki".format(LANG)
WIKIMERGE_COLLECTION = "{}wiki_merged".format(LANG)

QA_COLLECTION = "{}wiki_omer".format(LANG)
SRL_COLLECTION = "{}wiki_srl".format(LANG)

TOKENIZER = SpacyTokenizer(LANG, disable=['parser', 'ner', 'textcat', 'tagger'])
DATE_FORMATTER = DateFormatterFactory.get_formatter(lang=LANG, out_locale=LOCALE)

