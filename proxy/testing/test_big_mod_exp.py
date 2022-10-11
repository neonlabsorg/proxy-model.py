#  test eip-198 implementation

import unittest
import os
import json
import random
from typing import List
import eth_utils
from logged_groups import logged_group
from web3 import Web3
from solcx import compile_source
from web3.types import TxReceipt, HexBytes

from .testing_helpers import create_account, create_signer_account, request_airdrop, SolidityContractDeployer

proxy_url = os.environ.get('PROXY_URL', 'http://localhost:9090/solana')
proxy = Web3(Web3.HTTPProvider(proxy_url))
eth_account = proxy.eth.account.create('eip-198')
proxy.eth.default_account = eth_account.address

SOLIDITY_CONTRACT = '''
pragma solidity >=0.7.0 <0.9.0;

contract ModularCheck {
    event Result(uint256 val);

    function modExp(uint256 _b, uint256 _e, uint256 _m) public returns (uint256 result) {
        uint256 val = 0;
        assembly {
            // Free memory pointer
            let pointer := mload(0x40)

            // Define length of base, exponent and modulus. 0x20 == 32 bytes
            mstore(pointer, 0x20)
            mstore(add(pointer, 0x20), 0x20)
            mstore(add(pointer, 0x40), 0x20)

            // Define variables base, exponent and modulus
            mstore(add(pointer, 0x60), _b)
            mstore(add(pointer, 0x80), _e)
            mstore(add(pointer, 0xa0), _m)

            // Store the result
            let value := mload(0xc0)

            // Call the precompiled contract 0x05 = bigModExp
            if iszero(call(not(0), 0x05, 0, pointer, 0xc0, value, 0x20)) {
                revert(0, 0)
            }

            result := mload(value)
            val :=  result
        }
        emit Result(val);
    }
}
'''

TEST_DATA = list([
{
    "Base": "1111111111111111111111111111111111111111111111111111111111111111",
    "Exponent": "1111111111111111111111111111111111111111111111111111111111111111",
    "Modulus": "111111111111111111111111111111111111111111111111111111111111110A",
    "Expected": "0A7074864588D6847F33A168209E516F60005A0CEC3F33AAF70E8002FE964BCD"
},
{
    "Base": "2222222222222222222222222222222222222222222222222222222222222222",
    "Exponent": "2222222222222222222222222222222222222222222222222222222222222222",
    "Modulus": "1111111111111111111111111111111111111111111111111111111111111111",
    "Expected": "00"
},
{
    "Base": "3333333333333333333333333333333333333333333333333333333333333333",
    "Exponent": "3333333333333333333333333333333333333333333333333333333333333333",
    "Modulus": "2222222222222222222222222222222222222222222222222222222222222222",
    "Expected": "1111111111111111111111111111111111111111111111111111111111111111"
},
{
    "Base": "9874231472317432847923174392874918237439287492374932871937289719",
    "Exponent": "0948403985401232889438579475812347232099080051356165126166266222",
    "Modulus": "25532321a214321423124212222224222b242222222222222222222222222444",
    "Expected": "220ECE1C42624E98AEE7EB86578B2FE5C4855DFFACCB43CCBB708A3AB37F184D"
},
{
    "Base": "3494396663463663636363662632666565656456646566786786676786768766",
    "Exponent": "2324324333246536456354655645656616169896565698987033121934984955",
    "Modulus": "0218305479243590485092843590249879879842313131156656565565656566",
    "Expected": "012F2865E8B9E79B645FCE3A9E04156483AE1F9833F6BFCF86FCA38FC2D5BEF0"
},
{
    "Base": "0000000000000000000000000000000000000000000000000000000000000005",
    "Exponent": "0000000000000000000000000000000000000000000000000000000000000002",
    "Modulus": "0000000000000000000000000000000000000000000000000000000000000007",
    "Expected": "0000000000000000000000000000000000000000000000000000000000000004"
},
{
    "Base": "0000000000000000000000000000000000000000000000000000000000000019",
    "Exponent": "0000000000000000000000000000000000000000000000000000000000000019",
    "Modulus": "0000000000000000000000000000000000000000000000000000000000000064",
    "Expected": "0000000000000000000000000000000000000000000000000000000000000019"
},
{
    "Base": "7719472615821079694904732333912527190217998977709370935963838933860875309329",
    "Exponent": "7719472615821079694904732333912527190217998977709370935963838933860875309329",
    "Modulus": "7719472615821079694904732333912527190217998977709370935963838933860875309322",
    "Expected": "A7074864588D6847F33A168209E516F60005A0CEC3F33AAF70E8002FE964BCD"
},
])



class Test_big_mod_exp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\n\nhttps://neonlabs.atlassian.net/browse/NDEV-670")
        request_airdrop(eth_account.address, 300)
        print('eth_account.address:', eth_account.address)
        print('eth_account.key:', eth_account.key.hex())
        cls.deploy_solidity_contract(cls)

    def deploy_solidity_contract(self):
        compiled_sol = compile_source(SOLIDITY_CONTRACT)
        contract_id, contract_interface = compiled_sol.popitem()
        storage = proxy.eth.contract(abi=contract_interface['abi'], bytecode=contract_interface['bin'])
        trx_deploy = proxy.eth.account.sign_transaction(dict(
            nonce=proxy.eth.get_transaction_count(proxy.eth.default_account),
            chainId=proxy.eth.chain_id,
            gas=987654321,
            gasPrice=proxy.eth.gas_price,
            to='',
            value=0,
            data=storage.bytecode),
            eth_account.key
        )
        print('trx_deploy:', trx_deploy)
        self.trx_deploy_hash = proxy.eth.send_raw_transaction(trx_deploy.rawTransaction)
        print('trx_deploy_hash:', self.trx_deploy_hash.hex())
        trx_deploy_receipt = proxy.eth.wait_for_transaction_receipt(self.trx_deploy_hash)
        print('trx_deploy_receipt:', trx_deploy_receipt)

        self.deploy_block_hash = trx_deploy_receipt['blockHash']
        self.deploy_block_num = trx_deploy_receipt['blockNumber']
        print('deploy_block_hash:', self.deploy_block_hash)
        print('deploy_block_num:', self.deploy_block_num)

        self.storage_contract = proxy.eth.contract(
            address=trx_deploy_receipt.contractAddress,
            abi=storage.abi
        )

    def call_contract(self, base, exponent, modulus, expected):
        print("\ntest_big_mod_exp")
        #  check of the non-BPF syscall implementation by the eth_Call request
        eth_call_result = self.storage_contract.functions.modExp(base, exponent, modulus).call()
        self.assertEqual(eth_call_result, expected)

        #  check of the BPF syscall implementation by the eth_SendRawTransaction request
        nonce = proxy.eth.get_transaction_count(eth_account.address)
        tx = {'nonce': nonce}
        tx = self.storage_contract.functions.modExp(base, exponent, modulus).buildTransaction(tx)
        tx = proxy.eth.account.sign_transaction(tx, eth_account.key)
        tx_hash = proxy.eth.send_raw_transaction(tx.rawTransaction)
        tx_receipt = proxy.eth.wait_for_transaction_receipt(tx_hash)
        self.assertIsNotNone(tx_receipt)
        self.assertEqual(tx_receipt.status, 1)

        print('eth_call_result :', eth_call_result)

        for log in tx_receipt['logs']:
            for topic in log['topics']:
                print("topic:", topic)


    def test_big_mod_exp(self):
        for test in TEST_DATA:
            # b = int(test["Base"], 16)
            # e = int(test["Exponent"], 16)
            # m = int(test["Modulus"], 16)
            # r = int(test["Expected"], 16)

            b = bytes.fromhex(test["Base"])
            e = bytes.fromhex(test["Exponent"])
            m = bytes.fromhex(test["Modulus"])
            r = int(test["Expected"], 16)
            self.call_contract(b, e, m, r)


