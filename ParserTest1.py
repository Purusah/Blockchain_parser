import jsonrpcclient.tornado_client
import asyncio
import tornado.ioloop

jsonrpchost = "http://localhost:8080"
test = "http://190.2.148.11:50014"


class TestParser(object):

    def __init__(self, address):
        self.jsonrpc = jsonrpcclient.tornado_client.TornadoClient(jsonrpchost)
        self._tracked_addresses = set()  # O(1) add, remove, containing
        self._pending_transactions = asyncio.Queue()  # O(1) put and get
        self.address = address
        self.filter = ""

    # Tracked addresses
    @property
    def tracked_addresses(self):
        return self._tracked_addresses

    async def add_tracked_address(self, address):
        self._tracked_addresses.add(address)
        return self._tracked_addresses

    async def remove_tracked_address(self, address):
        try:
            self._tracked_addresses.remove(address)
        except KeyError:
            pass
        return self._tracked_addresses

    def size_tracked_addresses(self):
        return len(self._tracked_addresses)

    # Pending transactions
    async def get_pending_transaction(self):
        tx = await self._pending_transactions.get()
        return tx

    def get_pending_transaction_nowait(self):
        tx = self._pending_transactions.get_nowait()
        return tx

    async def put_pending_transaction(self, tx):
        await self._pending_transactions.put(tx)

    def size_pending_transactions(self):
        return self._pending_transactions.qsize()

    # Utils
    async def get_transaction_by_hash(self, tx_hash):
        method = "eth_getTransactionByHash"
        while True:
            response = await self.jsonrpc.request(method, tx_hash)
            if response.get("blockNumber"):
                break
            else:
                await asyncio.sleep(1)
                # print("ping")

        # print(response)
        return response

    async def make_block_filter(self):
        method = "eth_newBlockFilter"
        response = await self.jsonrpc.request(method)
        self.filter = response

    async def get_block_filter_changes(self):
        if not self.filter:
            await self.make_block_filter()

        method = "eth_getFilterChanges"
        filter_id = self.filter

        response = await self.jsonrpc.request(method, filter_id)
        # print("get_address_filter_changes response: ", response)
        if response:
            for block in response:
                await self.get_block_transactions(block)
            response = await self.parse_pending_transactions()
            return response

    async def get_block_transactions(self, block_hash):
        method = "eth_getBlockByHash"
        block = await self.jsonrpc.request(method, block_hash, True)
        for tx in block.get("transactions"):
            await self.put_pending_transaction(tx)

    async def parse_pending_transactions(self):
        transactions_set = []
        for i in range(self.size_pending_transactions()):
            transaction = await self.get_pending_transaction()
            from_is_target_address = (transaction.get("from").lower() == self.address.lower())
            to_is_target_address = (transaction.get("to").lower() == self.address.lower())
            # print(f"FROM: {transaction.get('from')} == Address: {self.address} Result: {from_is_target_address}")
            # print(f"TO: {transaction.get('to')} == Address: {self.address} Result: {to_is_target_address}")

            if from_is_target_address or to_is_target_address:
                # print("I got transaction: ", transaction)
                transactions_set.append(transaction)
        return transactions_set


async def main():
    contract_address = "0x378e3bee9c38268aa9bdc78bce8c7915f1bcc3e4"
    p = TestParser(contract_address)
    loop = tornado.ioloop.IOLoop.current()
    while True:
        resp = await p.get_block_filter_changes()
        # print("we got resp: ", resp)
        await asyncio.sleep(5)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
