from collections import defaultdict

from pymongo import MongoClient


# noinspection Duplicates
def get_ids(lang):
  client = MongoClient("localhost", 27017)
  db = client["WikiReading"]
  wiki_srl = db["{}wiki_srl".format(lang.lower())]

  documents = wiki_srl.find({}, {"_id": 0})

  ids = set()
  for document in documents:
    sentences = document["sentences"]

    for sid in sentences:
      sentence = sentences[sid]["sentence"]
      if "is a" in sentence:
        continue
      for relation in sentences[sid]["relations"]:
        rid = relation['id']
        prop = relation["prop_id"]
        if prop == "P31":
          continue
        ids.add(rid)

  return ids


# noinspection Duplicates
def get_docs(lang, ids):
  client = MongoClient("localhost", 27017)
  db = client["WikiReading"]
  wiki_srl = db["{}wiki_srl".format(lang.lower())]

  documents = wiki_srl.find({}, {"_id": 0})

  res = {}
  for document in documents:
    sentences = document["sentences"]

    for sid in sentences:
      for relation in sentences[sid]["relations"]:
        rid = relation['id']
        if rid in ids:
          res[rid] = sentences[sid]

  return res


if __name__ == '__main__':
  all_ids = {}
  intersection = {}
  first = True
  for lang in ["EN", "ES", "DE"]:
    ids = get_ids(lang)

    if first:
      intersection = ids
      first = False

    intersection = intersection.intersection(ids)
    print(len(ids))
    print(len(intersection))

  results = defaultdict(lambda: {})
  inter_ids = set(list(intersection)[:1000])
  for lang in ["EN", "ES", "DE"]:
    res = get_docs(lang, inter_ids)

    for k in res:
      results[k][lang] = res[k]

  for x in results:
    for l in results[x]:
      print("{} || {} || {}".format(x, l, results[x][l]))

    print("\n")
