import path

from date_parsers import DateFormatter
from tokenizers import SpacyTokenizer


def set_lang(lang, ext_lang, locale):
    global LANG, EXT_LANG, LOCALE
    LANG = lang
    EXT_LANG = ext_lang
    LOCALE = locale


LANG, EXT_LANG, LOCALE = None, None, None
NUM_WORKERS = 8
CHUNK_SIZE = 100000

MONGO_IP = "localhost"
MONGO_PORT = 27017
DB = "WikiReading"
WIKIDATA_COLLECTION = "wikidata"
WIKIPEDIA_COLLECTION = "{}_wikipedia".format(LANG)
WIKIMERGE_COLLECTION = "{}_wikimerge".format(LANG)

TOKENIZER = SpacyTokenizer(LANG, disable=['parser', 'ner', 'textcat', 'tagger'])
DATE_FORMATTER = DateFormatter(lang=LANG, out_locale=LOCALE)

VOCABS_FOLDER = "vocabs"

ANSWER_VOCAB_PATH = path.join(VOCABS_FOLDER, "answer.vocab")
DOCUMENT_VOCAB_PATH = path.join(VOCABS_FOLDER, "document.vocab")
RAW_ANSWER_VOCAB_PATH = path.join(VOCABS_FOLDER, "raw_answer.vocab")
TYPE_VOCAB_PATH = path.join(VOCABS_FOLDER, "type.vocab")
CHAR_VOCAB_OUT = path.join(VOCABS_FOLDER, "char.vocab")
