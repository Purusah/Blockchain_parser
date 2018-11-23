import json
import web3
import time

from web3 import Web3
from solc import compile_source
from web3.contract import ConciseContract

# # Solidity source code
# contract_source_code = '''
# pragma solidity ^0.4.21;
#
# contract Greeter {
#     string public greeting;
#
#     function Greeter() public {
#         greeting = 'Hello';
#     }
#
#     function setGreeting(string _greeting) public {
#         greeting = _greeting;
#     }
#
#     function greet() view public returns (string) {
#         return greeting;
#     }
# }
# '''
#
# compiled_sol = compile_source(contract_source_code) # Compiled source code
# contract_interface = compiled_sol['<stdin>:Greeter']
#
# # web3.py instance
# # w3 = Web3(Web3.EthereumTesterProvider())
w3 = Web3(Web3.IPCProvider("/home/purusah/mychain/data/geth.ipc"))
#
# # set pre-funded account as sender
w3.eth.defaultAccount = w3.eth.accounts[0]
w3.personal.unlockAccount(w3.personal.listAccounts[0], "1234", 15000)
#
#
# # Instantiate and deploy contract
# Greeter = w3.eth.contract(abi=contract_interface['abi'], bytecode=contract_interface['bin'])

# print("abi: ", contract_interface['abi'])
# print("bytecode: ", contract_interface['bin'])
# print("address, ",  Greeter.address)



# # Submit the transaction that deploys the contract
# tx_hash = Greeter.constructor().transact()
#
# # Wait for the transaction to be mined, and get the transaction receipt
# tx_receipt = w3.eth.waitForTransactionReceipt(tx_hash)
#
# # Create the contract instance with the newly-deployed address
# greeter = w3.eth.contract(
#     address=tx_receipt.contractAddress,
#     abi=contract_interface['abi'],)#
greeter = w3.eth.contract(
    address="0x378e3bee9C38268aa9bdc78bcE8c7915f1bCc3E4",
    abi=[{'constant': False, 'inputs': [{'name': '_greeting', 'type': 'string'}], 'name': 'setGreeting', 'outputs': [], 'payable': False, 'stateMutability': 'nonpayable', 'type': 'function'}, {'constant': True, 'inputs': [], 'name': 'greet', 'outputs': [{'name': '', 'type': 'string'}], 'payable': False, 'stateMutability': 'view', 'type': 'function'}, {'constant': True, 'inputs': [], 'name': 'greeting', 'outputs': [{'name': '', 'type': 'string'}], 'payable': False, 'stateMutability': 'view', 'type': 'function'}, {'inputs': [], 'payable': False, 'stateMutability': 'nonpayable', 'type': 'constructor'}],
)
# print("ADDRESS: ", tx_receipt.contractAddress)
print("wait")
time.sleep(10)
print("GO")

# Display the default greeting from the contract
print('Default contract greeting: {}'.format(
    greeter.functions.greet().call()
))

# for i in range(10):
#
#     print('Setting the greeting to Nihao...')
#     tx_hash = greeter.functions.setGreeting('Nihao').transact()
#     # Wait for transaction to be mined...
#     w3.eth.waitForTransactionReceipt(tx_hash)
#     print("TX HAH: ", tx_hash.hex())
#     time.sleep(5)
#     print('Updated contract greeting: {}'.format(
#         greeter.functions.greet().call()
#     ))

for i in range(1):

    print('Setting the greeting to Nihao...')
    for i in range(1000):
        tx_hash = greeter.functions.setGreeting('Nihao').transact()
        # Wait for transaction to be mined...
        # w3.eth.waitForTransactionReceipt(tx_hash)
        # print("TX HAH: ", tx_hash.hex())
        # time.sleep(5)
        # print('Updated contract greeting: {}'.format(
        #     greeter.functions.greet().call()
        # ))

    print("DOME")



# Display the new greeting value


# When issuing a lot of reads, try this more concise reader:
reader = ConciseContract(greeter)
assert reader.greet() == "Nihao"