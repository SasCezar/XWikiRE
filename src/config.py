from date_formatter import DateFormatterFactory

LANG = 'en'
LANGUAGE = 'english'
LOCALE = 'en-US'

NUM_WORKERS = 5
CHUNK_SIZE = 1000

MONGO_IP = "localhost"
MONGO_PORT = 27017
DB = "WikiQA"
WIKIDATA_COLLECTION = "wikidata"
WIKIPEDIA_COLLECTION = "{}wiki".format(LANG)
WIKIMERGE_COLLECTION = "{}wiki_merged".format(LANG)
QAMERGE_COLLECTION = "{}wiki_qa".format(LANG)

DATE_FORMATTER = DateFormatterFactory.get_formatter(lang=LANG, out_locale=LOCALE)
