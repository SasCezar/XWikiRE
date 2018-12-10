import hashlib
import time
import traceback
from abc import ABC, abstractmethod
from collections import Counter

from pymongo import MongoClient


class Builder(ABC):
    def __init__(self, ip, port, db, source, destination, batch_size=500):
        self._client = MongoClient(ip, port)
        self._db = self._client[db]
        self._source = self._db[source]
        self._destination = self._db[destination]
        self._batch_size = batch_size

    def build(self, key, limit, **kwargs):
        n = 0
        processed = []
        mask = kwargs['mask'] if 'mask' in kwargs else {"_id": 0}
        start_time = time.time()
        counter = Counter()
        for doc in self._source.find({key: {"$gte": limit[0], "$lte": limit[1]}}, mask):
            try:
                result = self._build(doc)
            except:
                traceback.print_exc()
                continue

            if result:
                document = result['document']
                counter.update(result['stats'])
                processed.append(document)
                n += 1

            if len(processed) >= self._batch_size:
                self._destination.insert_many(processed, ordered=False, bypass_document_validation=True)
                n += len(processed)
                processed = []

        if processed:
            self._destination.insert_many(processed, ordered=False, bypass_document_validation=True)
            n += len(processed)
        elapsed = int(time.time() - start_time)
        res = {"processed": n, "elapsed": elapsed}
        res.update(counter)

        return res

    @abstractmethod
    def _build(self, doc, **kwargs):
        return doc

    @staticmethod
    def _get_id(string):
        return hashlib.sha1(string.encode("utf-8")).hexdigest()


