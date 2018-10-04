LANG = "fr"
EXT_LANG = "french"
NUM_WORKERS = 16

MONGO_IP = "localhost"
MONGO_PORT = 27017
DB = "WikiReading"
WIKIDATA_COLLECTION = "wikidata"
WIKIPEDIA_COLLECTION = "wikipedia"
WIKIMERGE_COLLECTION = "wikimerge"

# TODO Complete for each used language
STOP_SECTIONS = {
    'en': ['See also', 'Notes', 'Further reading', 'External links'],
    'fr': ['Notes et références', 'Bibliographie', 'Voir aussi', 'Annexes', 'Références'],
    'it': []
}
