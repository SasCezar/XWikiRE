# X-WikiRE
This tool provides a semi-automated creation of the WikiReading dataset as described in the work of [Hewlett et. al.](https://arxiv.org/abs/1608.03542). There are some already built dataset available at their [repository](https://github.com/google-research-datasets/wiki-reading).

## Requirements
1. MongoDB
2. Python

## Procedure
### Required files

1. Download Wikidata JSON dump from [here](https://www.wikidata.org/wiki/Wikidata:Database_download)
2. Download Wikipedia XML dump from [here](https://dumps.wikimedia.org/backup-index.html)
3. Download the language specific page_props.sql dump from wikipedia dumps [here](https://dumps.wikimedia.org/backup-index.html)

### Data Processing

6. Build the mapping dict between Wikipedia IDs and WIkidata IDs using wiki_prop.py
7. Transform the XML dump to JSON using the segment_wiki.py (a custom version of Gensim's script described [here](https://radimrehurek.com/gensim/scripts/segment_wiki.html))


### Data import
2. Import the Wikidata dump into MongoDB in it's own collection using: 
    ```bash
    mongoimport --db WikiReading --collection wikidata --file wikidata_dump.json --jsonArray
    ```
3. Create an index on the "id" field
    ```
    db.wikidata.createIndex({"id": 1})
    ```
8. Import the JSON wikipedia dump into MongoDB
9. Create an index on the title field:
    ```
    db.wikidata.createIndex({"wikidata_id": 1})
    ```

### POS Tagger training
10. Train POS tagger for the desired language using [this](https://github.com/bplank/bilstm-aux), and the data from [universal dependencies](http://universaldependencies.org/)

## Cite

```
@inproceedings{abdou-etal-2019-x,
    title = "X-{W}iki{RE}: A Large, Multilingual Resource for Relation Extraction as Machine Comprehension",
    author = "Abdou, Mostafa  and
      Sas, Cezar  and
      Aralikatte, Rahul  and
      Augenstein, Isabelle  and
      S{\o}gaard, Anders",
    booktitle = "Proceedings of the 2nd Workshop on Deep Learning Approaches for Low-Resource NLP (DeepLo 2019)",
    month = nov,
    year = "2019",
    address = "Hong Kong, China",
    publisher = "Association for Computational Linguistics",
    url = "https://www.aclweb.org/anthology/D19-6130",
    doi = "10.18653/v1/D19-6130",
    pages = "265--274",
    abstract = "Although the vast majority of knowledge bases (KBs) are heavily biased towards English, Wikipedias do cover very different topics in different languages. Exploiting this, we introduce a new multilingual dataset (X-WikiRE), framing relation extraction as a multilingual machine reading problem. We show that by leveraging this resource it is possible to robustly transfer models cross-lingually and that multilingual support significantly improves (zero-shot) relation extraction, enabling the population of low-resourced KBs from their well-populated counterparts.",
}
```
