import logging
import multiprocessing
import time

from natural.date import compress
from pymongo import MongoClient

import config
from builder import OmerBuilder
from utils import get_chunks


def omer(limit):
    builder = OmerBuilder(config.MONGO_IP, config.MONGO_PORT, config.DB, config.WIKIMERGE_COLLECTION,
                          config.SRLMERGE_COLLECTION, config.LANGUAGE)
    return builder.build("id", limit)


def run_parallel():
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIPEDIA_COLLECTION]
    wikidocs = list(wikipedia.find({}, {'wikidata_id': 1, '_id': 0}).sort('wikidata_id'))
    chunks = get_chunks(wikidocs, config.CHUNK_SIZE, 'wikidata_id')
    del wikidocs
    start_time = time.time()
    total = 0

    pool = multiprocessing.Pool(config.NUM_WORKERS)
    for res in pool.imap(omer, chunks):
        total += res['processed']
        res['total'] = total
        part = int(time.time() - start_time)
        res['elapsed'] = compress(res['elapsed'])
        res['total_elapsed'] = compress(part)
        logging.info("Processed {processed} ({total} in total) documents in {elapsed} (running time {"
                     "total_elapsed})".format(**res))

    pool.terminate()

    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {}".format(total, compress(elapsed)))
    return


if __name__ == '__main__':
    run_parallel()
