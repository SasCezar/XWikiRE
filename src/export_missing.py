import json
import logging
import multiprocessing
import sys

from pymongo import MongoClient
from tqdm import tqdm

import config


def chunks(l, n):
  """Yield successive n-sized chunks from l."""
  for i in range(0, len(l), n):
    yield l[i:i + n]


def get_nodes(path):
  with open(path, "rt", encoding="utf8") as outf:
    res = set([a.strip() for a in outf.readlines()])

  print(len(res))
  return sorted(res)


def create_fact(doc):
  doc["_id"] = doc["id"]
  del doc["id"]
  doc["value"] = doc["labels"][config.LANG]["value"]
  doc["type"] = "entity"
  return doc


def run(sub):
  client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
  db = client[config.DB]
  wikidata = db[config.WIKIDATA_COLLECTION]
  dd = list(wikidata.find({"id": {"$in": sub}, "labels.en": {"$exists": True}},
                          {"_id": 0, "sitelinks": 0, "type": 0, "claims": 0}))

  res = []
  for doc in dd:
    try:
      clean_doc = create_fact(doc)
      res.append(clean_doc)
    except KeyError:
      continue


  return res

def export_missing(path):
  ids = get_nodes(path)

  chks = list(chunks(ids, 10000))
  with open("entities_objs.json", "wt", encoding="utf8") as outf, tqdm(total=len(ids)) as pbar:
    pool = multiprocessing.Pool(config.NUM_WORKERS)
    for res in pool.imap(run, chks):
      for x in res:
        outf.write(json.dumps(x, ensure_ascii=False) + "\n")


      pbar.update(10000)


if __name__ == '__main__':
  logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
  logging.info("Running %s", " ".join(sys.argv))
  path = "entities_ids.txt"
  export_missing(path)
  logging.info("Completed %s", " ".join(sys.argv))
