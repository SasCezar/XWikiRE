import logging
import multiprocessing
import sys
import time
from functools import partial

from natural.date import compress
from pymongo import MongoClient

import config
from builder import SRLBuilder
from utils import get_chunks


def build(limit, configs):
    builder = SRLBuilder(config.MONGO_IP, config.MONGO_PORT, config.DB, config.WIKIMERGE_COLLECTION,
                         config.SRLMERGE_COLLECTION, config.LANG)

    return builder.build("id", ['Q30', 'Q312', 'Q355', 'Q95', 'Q19571648', 'Q513'])


def build_srl(configs):
    client = MongoClient(config.MONGO_IP, config.MONGO_PORT)
    db = client[config.DB]
    wikipedia = db[config.WIKIMERGE_COLLECTION]
    documents_id = list(wikipedia.find({}, {"id": 1, "_id": 0}).sort("id"))
    client.close()
    start_time = time.time()
    total = 0
    tot_neg_examples = 0
    tot_pos_examples = 0
    chunks = get_chunks(documents_id, config.CHUNK_SIZE, 'id')
    if config.NUM_WORKERS == 1:
        for chunk in chunks:
            build(chunk, {})
    else:
        pool = multiprocessing.Pool(config.NUM_WORKERS)

        for res in pool.imap(partial(build, configs=configs), chunks):
            total += res['processed']
            res['total'] = total
            elapsed = int(time.time() - start_time)
            res['total_elapsed'] = compress(elapsed)
            res['elapsed'] = compress(res['elapsed'])
            logging.info(', '.join("{!s}={!r}".format(key, val) for key, val in res.items()))

        pool.terminate()
    elapsed = int(time.time() - start_time)
    logging.info("Processed {} documents in {} - Neg examples {} - Pos examples {}".format(total, compress(elapsed),
                                                                                           tot_neg_examples,
                                                                                           tot_pos_examples))
    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Running %s", " ".join(sys.argv))
    build_srl({})
    logging.info("Completed %s", " ".join(sys.argv))
