# WikiReading
This tool provides a semi-automated creation of the WikiReading dataset as described in the work of [Hewlett et. al.](https://arxiv.org/abs/1608.03542). There are some already built dataset available at their [repository](https://github.com/google-research-datasets/wiki-reading).

## Requirements
1. MongoDB
2. Python

## Procedure
1. Download Wikidata JSON dump from [here](https://www.wikidata.org/wiki/Wikidata:Database_download)
2. Import the Wikidata dump into MongoDB in it's own collection using: 
    ```bash
    mongoimport --db WikiReading --collection wikidata --file wikidata_dump.json --jsonArray
    ```
3. Create an index on the "id" field
    ```
    db.wikidata.createIndex({"id": 1})
    ```
4. Download Wikipedia XML dump from [here](https://dumps.wikimedia.org/backup-index.html)
5. Download the language specific page_props.sql dump from wikipedia dumps [here](https://dumps.wikimedia.org/backup-index.html)
6. Build the mapping dict between Wikipedia IDs and WIkidata IDs using wiki_prop.py
7. Transform the XML dump to JSON using the segment_wiki.py (a custom version of Gensim's script described [here](https://radimrehurek.com/gensim/scripts/segment_wiki.html))
8. Import the JSON wikipedia dump into MongoDB
9. Create an index on the title field:
    ```
    db.wikidata.createIndex({"wikidata_id": 1})
    ```
10. Train POS tagger for the desired language using [this](https://github.com/bplank/bilstm-aux), and the data from [universal dependencies](http://universaldependencies.org/)
11. Run the merging script using:
    ```
    ```
