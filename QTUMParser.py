import jsonrpcclient.tornado_client

import connectors
import containers

import os
import sys
import time
import logging
import multiprocessing as mp
# import ParserServer

jsonrpchost = "http://localhost:13889"  # TESTNET PORT. REAL: 3889
# test = "http://190.2.148.11:50014"

logging.basicConfig(format='%(asctime)s %(message)s',
                    filename='storage.log',
                    level=logging.DEBUG)


# TODO CHECK FOR BROKEN CONNECTION. IT MAY CRASH


class Balancer(object):
    def __init__(self,
                 jsonrpchost=jsonrpchost,
                 transactions_per_worker=200,
                 maximum_workers = 8,
                 tx_container=containers.ContainerQueue,
                 main_container=connectors.ConnectorIn(),
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
            kwargs = {"connector_in": connectors.ConnectorIn,  # "lock": self.lock,
                      "connector_out": connectors.ConnectorOut,
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

    def __init__(self, connector_in, connector_out, transaction_storage):  # lock

        # self._tracked_addresses = set()  # O(1) add, remove, containing
        # self._pending_transactions = asyncio.Queue()  # O(1) put and get
        # self.filter = ""
        # self.lock = lock

        # Contain addresses and workers data
        self.connector_in = connector_in
        self.connector_out = connector_out
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
        return self.connector_in.check_worker_required(self.pid)

    def worker_register(self):
        # Creates document in storage that worker exist and working
        logging.debug(f"Worker {self.pid} registered.")
        self.connector_in.add_worker(self.pid)

    def exit(self):
        # Worker shut down himself to free resources
        logging.debug(f"Worker {self.pid} STOPPED")
        self.connector_in.worker_suicide(self.pid)
        sys.exit(0)

    def parse_pending_transactions(self):
        transaction = self.transaction_storage.get()
        if transaction:
            from_is_target_address = self.connector_in.\
                check_address(transaction.get("from").lower())

            to_is_target_address = self.connector_in.\
                check_address(transaction.get("to").lower())

            print(f"FROM: {transaction.get('from')} Result: {from_is_target_address}")
            print(f"TO: {transaction.get('to')} Result: {to_is_target_address}")

            if from_is_target_address or to_is_target_address:
                logging.debug(f"TRANSACTION: {transaction}\n")
                # print(f"TRANSACTION: {transaction}\n")
                self.connector_out.save_transaction(transaction)
        else:
            print(f"NO transactions. Wait. Worker {self.pid}")
            time.sleep(2)


def main(start_block=None):
    out = connectors.ConnectorIn()
    b = Balancer(jsonrpchost, start_block=start_block)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        out = connectors.ConnectorIn()
        out.prepare_storage()
