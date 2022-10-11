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
    "Base":     7719472615821079694904732333912527190217998977709370935963838933860875309329,
    "Exponent": 7719472615821079694904732333912527190217998977709370935963838933860875309329,
    "Modulus":  7719472615821079694904732333912527190217998977709370935963838933860875309322,
    "Expected": 4721819579619764028748889379605432748696806372499657368655739696405823245261
},
{
    "Base":     15438945231642159389809464667825054380435997955418741871927677867721750618658,
    "Exponent": 15438945231642159389809464667825054380435997955418741871927677867721750618658,
    "Modulus":  7719472615821079694904732333912527190217998977709370935963838933860875309329,
    "Expected": 0
},
{
    "Base":     23158417847463239084714197001737581570653996933128112807891516801582625927987,
    "Exponent": 23158417847463239084714197001737581570653996933128112807891516801582625927987,
    "Modulus":  15438945231642159389809464667825054380435997955418741871927677867721750618658,
    "Expected": 7719472615821079694904732333912527190217998977709370935963838933860875309329
},
{
    "Base":     68956749356517800030681449047466693632135990348080009676796705356563589142297,
    "Exponent": 4198471888429487897682814501579645141410074458948413511103935208441164227106,
    "Modulus":  16882466171826739201269145272988084451510343959194115005957945952155132372036,
    "Expected": 15404795232354952533020054382394563327585295675709887348164544992612326512717
},
{
    "Base":     23782157651828180791157064017186648244179383020374656253598549321330680301414,
    "Exponent": 15894903093766010713733242511548669081670931206767744756151308446510324664661,
    "Modulus":  947363587939062894675645306794008142782204445957472674892858962177404790118,
    "Expected": 535633477945263887611390916462803759882789435598733198638899124129215397616
},
{
    "Base": 5,
    "Exponent": 2,
    "Modulus": 7,
    "Expected": 4
},
{
    "Base": 25,
    "Exponent": 25,
    "Modulus": 100,
    "Expected": 25
}
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
            data = int(log["data"][2:], 16)
            print("data:", data)


    def test_big_mod_exp(self):
        for test in TEST_DATA:
            # b = test["Base"],
            # e = int(test["Exponent"], 16)
            # m = int(test["Modulus"], 16)
            # r = int(test["Expected"], 16)

            self.call_contract(test["Base"], test["Exponent"], test["Modulus"], test["Expected"])


