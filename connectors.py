import pymongo

import config

import time


class ConnectorIn(object):
    def __init__(self, coinid, host=None, **kwargs):
        if coinid not in config.coins:
            raise ValueError("Wrong coinid to create ConnectorIn")

        database_name = config.databases[coinid]
        self.DB_client = pymongo.MongoClient()
        self.addresses = self.DB_client.database_name.addresses
        self.workers = self.DB_client.database_name.workers
        self.transactions = self.DB_client.database_name.transactions

    def check_address(self, address):
        print("Check address: ", address)
        res = self.addresses.find_one({"address": address})
        print("Check address: ", res)
        if res:
            return True
        else:
            return False

    def check_worker_alive(self, pid):
        res = self.workers.find_one({"pid": pid})
        if res.get("is_alive"):
            return True
        else:
            return False

    def check_worker_required(self, pid):
        res = self.workers.find_one({"pid": pid})
        if res.get("is_required"):
            return True
        else:
            return False

    def stop_worker(self):
        worker = self.workers.find_one({"is_alive": True, "is_required": True},
                                       sort=[("timestamp", pymongo.DESCENDING)])

        self.workers.update_one({"pid": worker.get("pid")},
                                {"$set":
                                     {"is_required": False}})

    def add_address(self, address):
        self.addresses.insert_one({"address": address})

    def add_worker(self, pid):
        if self.workers.find_one({"pid": pid}):
            self.workers.update_one({"pid": pid},
                                    {"$set":
                                         {"is_alive": True,
                                          "is_required": True,
                                          "timestamp": time.time()}})
        else:
            self.workers.insert_one({"pid": pid,
                                     "is_alive": True,
                                     "is_required": True,
                                     "timestamp": time.time()})

    def workers_number(self):
        number = self.workers.count_documents({"is_alive": True,
                                               "is_required": True})
        return number

    def worker_suicide(self, pid):
        self.workers.update_one({"pid": pid},
                                {"$set":
                                     {"is_required": False,
                                      "is_alive": False}})

    def prepare_storage(self):
        print([x for x in self.workers.find({})])
        self.workers.delete_many({})
        self.workers.create_index("pid")
        self.addresses.create_index(keys="address", unique=True, background=True)


class ConnectorOut(object):
    def __init__(self, coinid, host=None, **kwargs):

        if coinid not in config.coins:
            raise ValueError("Wrong coinid to create ConnectorOut")

        database_name = config.databases[coinid]
        self.DB_client = pymongo.MongoClient()
        self.transactions = self.DB_client.database_name.transactions

    def save_transaction(self, transaction):
        self.transactions.insert_one(transaction)
