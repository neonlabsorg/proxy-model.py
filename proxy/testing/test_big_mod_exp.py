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

    function modExp() public returns (uint256 result) {
        uint256 _b = 7719472615821079694904732333912527190217998977709370935963838933860875309329;
        uint256 _e = 7719472615821079694904732333912527190217998977709370935963838933860875309329;
        uint256 _m = 7719472615821079694904732333912527190217998977709370935963838933860875309322;
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
        }
}
'''



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

    def test_big_mod_exp(self):
        print("\ntest_big_mod_exp")
        number = self.storage_contract.functions.modExp(5, 2, 7).call()


        nonce = proxy.eth.get_transaction_count(eth_account.address)
        tx = {'nonce': nonce}
        tx = self.storage_contract.functions.modExp(5, 2, 7).buildTransaction(tx)
        tx = proxy.eth.account.sign_transaction(tx, eth_account.key)
        tx_hash = proxy.eth.send_raw_transaction(tx.rawTransaction)
        tx_receipt = proxy.eth.wait_for_transaction_receipt(tx_hash)
        self.assertIsNotNone(tx_receipt)
        self.assertEqual(tx_receipt.status, 1)

        print("tx_receipt: ", tx_receipt)
        # print('result 5^2 mod 7 :', number)
        # self.assertEqual(number, 4) # 5^2 % 7 = 4
