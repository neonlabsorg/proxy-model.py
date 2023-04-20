import unittest

from unittest.mock import patch
from typing import Optional

from ..common_neon.config import Config
from ..common_neon.solana_tx import SolPubKey
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.address import NeonAddress

from ..gas_tank import GasTank
from ..gas_tank.portal_analyzer import PortalTxAnalyzer
from ..gas_tank.neon_pass_analyzer import NeonPassAnalyzer
from ..gas_tank.gas_less_accounts_db import GasLessAccountsDB

from ..indexer.sql_dict import SQLDict

from ..testing.transactions import neon_pass_tx, neon_pass_whitelist, evm_loader_addr, neon_pass_gas_less_account
from ..testing.transactions import write_wormhole_redeem_tx, execute_wormhole_redeem_tx, wormhole_gas_less_account


class FakeConfig(Config):
    def __init__(self, start_slot: str):
        super().__init__()
        self._start_slot = start_slot
        self._pyth_mapping_account = SolPubKey.new_unique()

    @property
    def pyth_mapping_account(self) -> Optional[SolPubKey]:
        return self._pyth_mapping_account

    @property
    def fuzz_fail_pct(self) -> int:
        return 0


class TestGasTank(unittest.TestCase):
    @classmethod
    def create_gas_tank(cls, start_slot: str):
        config = FakeConfig(start_slot)
        neon_pass_analyzer = NeonPassAnalyzer(config, neon_pass_whitelist)
        # portal_tx_analyzer = PortalTxAnalyzer(True)
        return GasTank(
            config=config,
            sol_tx_analyzer_dict={neon_pass_analyzer.name: neon_pass_analyzer},
            neon_tx_analyzer_dict={}
        )

    @classmethod
    @patch.object(SQLDict, 'get')
    @patch.object(SolInteractor, 'get_block_slot')
    def setUpClass(cls, mock_get_slot, mock_dict_get) -> None:
        print("testing indexer in gas-tank mode")
        cls.evm_loader_id = evm_loader_addr
        cls.gas_tank = cls.create_gas_tank('0')
        mock_get_slot.assert_called_once_with('finalized')
        mock_dict_get.assert_called()

    @patch.object(NeonPassAnalyzer, '_is_allowed_contract')
    @patch.object(GasLessAccountsDB, 'add_gas_less_permit_list')
    def test_failed_permit_contract_not_in_whitelist(self, mock_add_gas_less_permit, mock_is_allowed_contract):
        """ Should not permit gas-less txs for contract that is not in whitelist """
        self.gas_tank._current_slot = 1
        mock_is_allowed_contract.side_effect = [False]

        self.gas_tank._process_sol_tx(neon_pass_tx)
        self.gas_tank._save_cached_data()

        mock_is_allowed_contract.assert_called_once()
        mock_add_gas_less_permit.assert_not_called()

    @patch.object(GasTank, '_has_gas_less_tx_permit')
    @patch.object(GasLessAccountsDB, 'add_gas_less_permit_list')
    def test_not_permit_for_already_processed_address(self, mock_add_gas_less_permit, mock_has_gas_less_tx_permit):
        """ Should not permit gas-less txs to repeated address """
        self.gas_tank._current_slot = 1
        mock_has_gas_less_tx_permit.side_effect = [True]

        self.gas_tank._process_sol_tx(neon_pass_tx)
        self.gas_tank._save_cached_data()

        mock_has_gas_less_tx_permit.assert_called_once()
        mock_add_gas_less_permit.assert_not_called()

    @patch.object(SQLDict, 'get')
    @patch.object(SolInteractor, 'get_block_slot')
    def test_init_gas_tank_slot_continue(self, mock_get_slot, mock_dict_get):
        start_slot = 1234
        mock_dict_get.side_effect = [start_slot - 1]
        mock_get_slot.side_effect = [start_slot + 1]

        new_gas_tank = self.create_gas_tank('CONTINUE')

        self.assertEqual(new_gas_tank._latest_gas_tank_slot, start_slot - 1)
        mock_get_slot.assert_called_once_with('finalized')
        mock_dict_get.assert_called()

    @patch.object(SQLDict, 'get')
    @patch.object(SolInteractor, 'get_block_slot')
    def test_init_gas_tank_slot_continue_recent_slot_not_found(self, mock_get_slot, mock_dict_get):
        start_slot = 1234
        mock_dict_get.side_effect = [None]
        mock_get_slot.side_effect = [start_slot + 1]

        new_gas_tank = self.create_gas_tank('CONTINUE')

        self.assertEqual(new_gas_tank._latest_gas_tank_slot, start_slot + 1)
        mock_get_slot.assert_called_once_with('finalized')
        mock_dict_get.assert_called()

    @patch.object(SQLDict, 'get')
    @patch.object(SolInteractor, 'get_block_slot')
    def test_init_gas_tank_start_slot_parse_error(self, mock_get_slot, mock_dict_get):
        start_slot = 1234
        mock_dict_get.side_effect = [start_slot - 1]
        mock_get_slot.side_effect = [start_slot + 1]

        new_gas_tank = self.create_gas_tank('Wrong value')

        self.assertEqual(new_gas_tank._latest_gas_tank_slot, start_slot - 1)
        mock_get_slot.assert_called_once_with('finalized')
        mock_dict_get.assert_called()

    @patch.object(SQLDict, 'get')
    @patch.object(SolInteractor, 'get_block_slot')
    def test_init_gas_tank_slot_latest(self, mock_get_slot, mock_dict_get):
        start_slot = 1234
        mock_dict_get.side_effect = [start_slot - 1]
        mock_get_slot.side_effect = [start_slot + 1]

        new_gas_tank = self.create_gas_tank('LATEST')

        self.assertEqual(new_gas_tank._latest_gas_tank_slot, start_slot + 1)
        mock_get_slot.assert_called_once_with('finalized')
        mock_dict_get.assert_called()

    @patch.object(SQLDict, 'get')
    @patch.object(SolInteractor, 'get_block_slot')
    def test_init_gas_tank_slot_number(self, mock_get_slot, mock_dict_get):
        start_slot = 1234
        mock_dict_get.side_effect = [start_slot - 1]
        mock_get_slot.side_effect = [start_slot + 1]

        new_gas_tank = self.create_gas_tank(str(start_slot))

        self.assertEqual(new_gas_tank._latest_gas_tank_slot, start_slot)
        mock_get_slot.assert_called_once_with('finalized')
        mock_dict_get.assert_called()

    @patch.object(SQLDict, 'get')
    @patch.object(SolInteractor, 'get_block_slot')
    def test_init_gas_tank_big_slot_number(self, mock_get_slot, mock_dict_get):
        start_slot = 1234
        mock_dict_get.side_effect = [start_slot - 1]
        mock_get_slot.side_effect = [start_slot + 1]

        new_gas_tank = self.create_gas_tank(str(start_slot + 100))

        self.assertEqual(new_gas_tank._latest_gas_tank_slot, start_slot + 1)
        mock_get_slot.assert_called_once_with('finalized')
        mock_dict_get.assert_called()

    def test_neonpass_tx_simple_case(self):
        """ Should allow gas-less txs to liquidity transfer in simple case by NeonPass"""
        self.gas_tank._current_slot = 2
        self.gas_tank._process_sol_tx(neon_pass_tx)
        self.gas_tank._save_cached_data()

        has_permit = self.gas_tank._gas_less_account_db.has_gas_less_tx_permit(NeonAddress(neon_pass_gas_less_account))
        self.assertTrue(has_permit)

    def test_wormhole_transaction_simple_case(self):
        """ Should allow gas-less txs to liquidity transfer in simple case by Wormhole"""

        self.gas_tank._current_slot = 2

        self.gas_tank._process_neon_ix(write_wormhole_redeem_tx)
        self.gas_tank._process_neon_ix(execute_wormhole_redeem_tx)
        self.gas_tank._save_cached_data()

        has_permit = self.gas_tank._gas_less_account_db.has_gas_less_tx_permit(NeonAddress(wormhole_gas_less_account))
        self.assertTrue(has_permit)
