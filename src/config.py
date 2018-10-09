from date_parsers import DateFormatter
from tokenizers import SpacyTokenizer

LANG = "fr"
EXT_LANG = "french"
NUM_WORKERS = 8

MONGO_IP = "localhost"
MONGO_PORT = 27017
DB = "WikiReading"
WIKIDATA_COLLECTION = "wikidata"
WIKIPEDIA_COLLECTION = "wikipedia"
WIKIMERGE_COLLECTION = "fr_wikimerge"


TOKENIZER = SpacyTokenizer(LANG, disable=['parser', 'ner', 'textcat', 'tagger'])
DATE_FORMATTER = DateFormatter(lang=LANG, out_locale='fr-FR')
