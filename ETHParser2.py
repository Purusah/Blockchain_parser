import jsonrpcclient.tornado_client
import asyncio
import tornado.ioloop
import queue
import multiprocessing as mp
import pymongo
import motor

import os
import sys
import time
import logging
# import ParserServer

jsonrpchost = "http://localhost:8080"
test = "http://190.2.148.11:50014"

logging.basicConfig(format='%(asctime)s %(message)s', filename='storage.log', level=logging.DEBUG)


# TODO CHECK FOR BROKEN CONNECTION. IT MAY CRASH
class ContainerQueue(object):
    def __init__(self, lock):
        self.container = mp.Queue()
        self.lock = lock

    def get(self):
        try:
            self.lock.acquire()
            tx = self.container.get_nowait()
        except queue.Empty:
            tx = None
        self.lock.release()
        return tx

    def put(self, item):
        self.lock.acquire()
        self.container.put_nowait(item)
        self.lock.release()

    def __len__(self):
        return self.container.qsize()


class DBClient(object):
    def __init__(self, host=None):
        self.DB_client = pymongo.MongoClient()
        self.addresses = self.DB_client.pmesETHBalancer.addresses
        self.workers = self.DB_client.pmesETHBalancer.workers
        self.transactions = self.DB_client.pmesETHBalancer.transactions

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

    def save_transaction(self, transaction):
        self.transactions.insert_one(transaction)


class Balancer(object):
    def __init__(self, jsonrpchost=jsonrpchost,
                 transactions_per_worker=200,
                 maximum_workers = 8,
                 tx_container=ContainerQueue,
                 main_container=DBClient,
                 start_block=None):

        self.lock = mp.Lock()
        # Gets blockchain transactions
        self.jsonrpc = jsonrpcclient.http_client.HTTPClient(jsonrpchost)
        # Allowed transaction to execute for worker
        self.transactions_per_worker = transactions_per_worker
        # Maximum workers
        self.maximum_workers = maximum_workers
        # Transaction container
        self.pending_transactions = tx_container(self.lock)
        # Addresses and worker container
        self.storage = main_container()
        # Required amount of worker to handle with blockchain transactions
        self.required_workers = self.estimate_required_workers()
        # Blockchain filter
        self.filter = ""
        # Defines block to start from (if future block it doesn't affect)
        self.start_block = start_block

        # Start work
        self.start()

    def start(self):
        self.storage.prepare_storage()
        self.create_new_worker()

        if isinstance(self.start_block, int):
            latest_block = self.get_current_block()
            if (self.start_block >= latest_block) or (self.start_block < 1):
                pass
            else:
                logging.debug("[+] - History revision required\n")
                current_working_block = self.start_block
                # self.gain_network()
                while current_working_block != self.get_current_block():
                    block = self.get_block_number(current_working_block)
                    for tx in block.get("transactions"):
                        self.pending_transactions.put(tx)
                    current_working_block += 1
        else:
            pass

        logging.debug("[+] - Start Main Loop\n")
        self.make_block_filter()

        while True:
            self.get_block_filter_changes()
            self.balancer()
            time.sleep(2)

    def estimate_required_workers(self):
        return len(self.pending_transactions) // self.transactions_per_worker + 1

    def balancer(self):
        current_workers = self.storage.workers_number()
        self.required_workers = self.estimate_required_workers()

        logging.debug(f"Balancer PING. "
                      f"Current workers: {current_workers}. "
                      f"Required workers: {self.required_workers}. "
                      f"Pending transactions: {len(self.pending_transactions)}")

        if current_workers > self.required_workers:
            logging.debug("To much workers.")
            self.kill_one_worker()
        elif current_workers < self.required_workers:
            logging.debug("We need more workers.")
            self.create_new_worker()
        else:
            pass
            # logging.debug("Just enough workers")
        logging.debug("")

    def make_block_filter(self):
        method = "eth_newBlockFilter"
        response = self.jsonrpc.request(method)
        self.filter = response

    def get_block_filter_changes(self):
        method = "eth_getFilterChanges"
        filter_id = self.filter

        response = self.jsonrpc.request(method, filter_id)
        print("get_address_filter_changes response: ", response)
        if response:
            for block in response:
                self.get_block_transactions(block)
                # response = self.parse_pending_transactions()
                # return response

    def get_block_transactions(self, block_hash):
        method = "eth_getBlockByHash"
        block = self.jsonrpc.request(method, block_hash, True)
        for tx in block.get("transactions"):
            self.pending_transactions.put(tx)

    def create_new_worker(self):
        if self.storage.workers_number() < self.maximum_workers:
            kwargs = {"main_storage": DBClient, # "lock": self.lock,
                      "transaction_storage": self.pending_transactions}
            mp.Process(target=ETHParser, args=(), kwargs=kwargs).start()

        else:
            logging.warning(f"[+] - Reached maximal amount of workers."
                            f"Transactions: {len(self.pending_transactions)}")

    def kill_one_worker(self):
        self.storage.stop_worker()

    def get_current_block(self):
        method = "eth_blockNumber"
        block_number = self.jsonrpc.request(method)
        print("Block number")
        return block_number

    def get_block_number(self, number):
        method = "eth_getBlockByNumber"
        block = self.jsonrpc.request(method, number, True)
        return block



class ETHParser(object):

    def __init__(self, main_storage, transaction_storage): # lock

        # self._tracked_addresses = set()  # O(1) add, remove, containing
        # self._pending_transactions = asyncio.Queue()  # O(1) put and get
        # self.filter = ""
        # self.lock = lock

        # Contain addresses and workers data
        self.main_storage = main_storage()
        # Contain transactions to workaround
        self.transaction_storage = transaction_storage
        # Unique number of current process
        self.pid = os.getpid()

        logging.debug(f"Created NEW worker. PID: {self.pid}")

        self.work()  # Main loop

    def work(self):
        self.worker_register()
        while True:
            logging.debug(f"PING: {self.pid}")
            self.parse_pending_transactions()
            if not self.is_requred():
                self.exit()
            # print("PINGPING: ", self.is_requred())

    def is_requred(self):
        # Check if current process still required or can be shut down
        return self.main_storage.check_worker_required(self.pid)

    def worker_register(self):
        # Creates document in storage that worker exist and working
        logging.debug(f"Worker {self.pid} registered.")
        self.main_storage.add_worker(self.pid)

    def exit(self):
        # Worker shut down himself to free resources
        logging.debug(f"Worker {self.pid} STOPPED")
        self.main_storage.worker_suicide(self.pid)
        sys.exit(0)

    def parse_pending_transactions(self):
        transaction = self.transaction_storage.get()
        if transaction:
            from_is_target_address = self.main_storage.check_address(transaction.get("from").lower())
            to_is_target_address = self.main_storage.check_address(transaction.get("to").lower())

            print(f"FROM: {transaction.get('from')} Result: {from_is_target_address}")
            print(f"TO: {transaction.get('to')} Result: {to_is_target_address}")

            if from_is_target_address or to_is_target_address:
                logging.debug(f"TRANSACTION: {transaction}\n")
                # print(f"TRANSACTION: {transaction}\n")
                self.main_storage.save_transaction(transaction)
        else:
            print(f"NO transactions. Wait. Worker {self.pid}")
            time.sleep(2)


# # Tracked addresses
    # @property
    # def tracked_addresses(self):
    #     return self._tracked_addresses
    #
    # async def add_tracked_address(self, address):
    #     self._tracked_addresses.add(address)
    #     return self._tracked_addresses
    #
    # async def remove_tracked_address(self, address):
    #     try:
    #         self._tracked_addresses.remove(address)
    #     except KeyError:
    #         pass
    #     return self._tracked_addresses
    #
    # def size_tracked_addresses(self):
    #     return len(self._tracked_addresses)
    #
    # # Pending transactions
    # async def get_pending_transaction(self):
    #     tx = await self._pending_transactions.get()
    #     return tx
    #
    # def get_pending_transaction_nowait(self):
    #     tx = self._pending_transactions.get_nowait()
    #     return tx
    #
    # async def put_pending_transaction(self, tx):
    #     await self._pending_transactions.put(tx)
    #
    # def size_pending_transactions(self):
    #     return self._pending_transactions.qsize()
    #
    # # Utils
    # async def get_transaction_by_hash(self, tx_hash):
    #     method = "eth_getTransactionByHash"
    #     while True:
    #         response = await self.jsonrpc.request(method, tx_hash)
    #         if response.get("blockNumber"):
    #             break
    #         else:
    #             await asyncio.sleep(1)
    #             # print("ping")
    #
    #     # print(response)
    #     return response
    #
    # async def make_block_filter(self):
    #     method = "eth_newBlockFilter"
    #     response = await self.jsonrpc.request(method)
    #     self.filter = response
    #
    # async def get_block_filter_changes(self):
    #     if not self.filter:
    #         await self.make_block_filter()
    #
    #     method = "eth_getFilterChanges"
    #     filter_id = self.filter
    #
    #     response = await self.jsonrpc.request(method, filter_id)
    #     # print("get_address_filter_changes response: ", response)
    #     if response:
    #         for block in response:
    #             await self.get_block_transactions(block)
    #         response = await self.parse_pending_transactions()
    #         return response
    #
    # async def get_block_transactions(self, block_hash):
    #     method = "eth_getBlockByHash"
    #     block = await self.jsonrpc.request(method, block_hash, True)
    #     for tx in block.get("transactions"):
    #         await self.put_pending_transaction(tx)
    #
    # async def parse_pending_transactions(self):
    #     transactions_set = []
    #     for i in range(self.size_pending_transactions()):
    #         transaction = await self.get_pending_transaction()
    #         from_is_target_address = (transaction.get("from").lower() == self.address.lower())
    #         to_is_target_address = (transaction.get("to").lower() == self.address.lower())
    #         # print(f"FROM: {transaction.get('from')} == Address: {self.address} Result: {from_is_target_address}")
    #         # print(f"TO: {transaction.get('to')} == Address: {self.address} Result: {to_is_target_address}")
    #
    #         if from_is_target_address or to_is_target_address:
    #             # print("I got transaction: ", transaction)
    #             transactions_set.append(transaction)
    #     return transactions_set


# class DBClient(object):
#     def __init__(self, host=None):
#         self.DB_client = motor.MotorClient()
#         self.addresses = self.DB_client.pmes_addresses
#         self.workers = self.DB_client.pmes_worker
#
#     async def check_address(self, address):
#         res = await self.addresses.find_one({"address": address})
#         if res:
#             return True
#         else:
#             return False
#
#     async def check_worker_state(self, pid):
#         res = await self.workers.find_one({"pid": pid})
#         if res:
#             return True
#         else:
#             return False
#
#     async def add_address(self, address):
#         await self.addresses.insert_one({"address": address})
#
#     async def add_worker(self, pid):
#         await self.addresses.insert_one({"pid": pid})


# async def main():
    # contract_address = "0x378e3bee9c38268aa9bdc78bce8c7915f1bcc3e4"
    # p = TestParser(contract_address)
    # loop = tornado.ioloop.IOLoop.current()
    # while True:
    #     resp = await p.get_block_filter_changes()
    #     # print("we got resp: ", resp)
    #     await asyncio.sleep(5)

def main(start_block=None):
    db = DBClient()
    b = Balancer(jsonrpchost, start_block=start_block)


    # contract_address = "0x378e3bee9c38268aa9bdc78bce8c7915f1bcc3e4"
    # p = TestParser(contract_address)
    # loop = tornado.ioloop.IOLoop.current()
    # while True:
    #     resp = await p.get_block_filter_changes()
    #     # print("we got resp: ", resp)
    #     await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        db = DBClient()
        db.prepare_storage()
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
