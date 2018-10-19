import os

from date_parsers import DateFormatterFactory
from tokenizers import SpacyTokenizer

LANG = 'es'
LOCALE = 'es-ES'

NUM_WORKERS = 6
CHUNK_SIZE = 1000

MONGO_IP = "localhost"
MONGO_PORT = 27017
DB = "WikiReading"
WIKIDATA_COLLECTION = "wikidata"
WIKIPEDIA_COLLECTION = "{}wiki".format(LANG)
WIKIMERGE_COLLECTION = "{}wiki_merged".format(LANG)
OMERMERGE_COLLECTION = "{}wiki_omer".format(LANG)

TOKENIZER = SpacyTokenizer(LANG, disable=['parser', 'ner', 'textcat', 'tagger'])
DATE_FORMATTER = DateFormatterFactory.get_formatter(lang=LANG, out_locale=LOCALE)

VOCABS_FOLDER = "vocabs"

ANSWER_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "answer.vocab")
DOCUMENT_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "document.vocab")
RAW_ANSWER_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "raw_answer.vocab")
TYPE_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "type.vocab")
CHAR_VOCAB_OUT = os.path.join(VOCABS_FOLDER, "char.vocab")
