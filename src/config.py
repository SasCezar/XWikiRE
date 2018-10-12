import os

from date_parsers import DateFormatter
from tokenizers import SpacyTokenizer

LANG = "fr"
EXT_LANG = "french"
LOCALE = 'fr-FR'
NUM_WORKERS = 6
CHUNK_SIZE = 1000

MONGO_IP = "localhost"
MONGO_PORT = 27017
DB = "WikiReading"
WIKIDATA_COLLECTION = "wikidata"
WIKIPEDIA_COLLECTION = "{}_wikipedia".format(LANG)
WIKIMERGE_COLLECTION = "{}_wikimerge_omer".format(LANG)

TOKENIZER = SpacyTokenizer(LANG, disable=['parser', 'ner', 'textcat', 'tagger'])
DATE_FORMATTER = DateFormatter(lang=LANG, out_locale=LOCALE)

VOCABS_FOLDER = "vocabs"

ANSWER_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "answer.vocab")
DOCUMENT_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "document.vocab")
RAW_ANSWER_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "raw_answer.vocab")
TYPE_VOCAB_PATH = os.path.join(VOCABS_FOLDER, "type.vocab")
CHAR_VOCAB_OUT = os.path.join(VOCABS_FOLDER, "char.vocab")
