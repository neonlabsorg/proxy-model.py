from __future__ import annotations

import abc
import dataclasses
import logging

from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterator, Optional, Any, List

import psycopg2.extensions
import requests

from ..common_neon.address import NeonAddress
from ..common_neon.config import Config
from ..common_neon.evm_log_decoder import NeonLogTxEvent
from ..common_neon.pythnetwork import PythNetworkClient
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.solana_neon_tx_receipt import SolTxReceiptInfo, SolNeonIxReceiptInfo
from ..common_neon.utils.neon_tx_info import NeonTxInfo
from ..common_neon.utils.json_logger import logging_context

from ..indexer.base_db import BaseDB
from ..indexer.indexed_objects import NeonIndexedHolderInfo, NeonIndexedTxInfo
from ..indexer.indexer_base import IndexerBase
from ..indexer.solana_tx_meta_collector import SolTxMetaDict, FinalizedSolTxMetaCollector
from ..indexer.sql_dict import SQLDict
from ..indexer.utils import check_error

LOG = logging.getLogger(__name__)

EVM_LOADER_CALL_FROM_RAW_TRX = 0x1f
EVM_LOADER_STEP_FROM_RAW_TRX = 0x20
EVM_LOADER_HOLDER_WRITE = 0x26
EVM_LOADER_TRX_STEP_FROM_ACCOUNT = 0x21
EVM_LOADER_TRX_STEP_FROM_ACCOUNT_NO_CHAINID = 0x22
EVM_LOADER_CANCEL = 0x23
EVM_LOADER_TRX_EXECUTE_FROM_ACCOUNT = 0x2A

ACCOUNT_CREATION_PRICE_SOL = Decimal('0.00472692')
AIRDROP_AMOUNT_SOL = ACCOUNT_CREATION_PRICE_SOL / 2


class FailedAttempts(BaseDB):
    def __init__(self) -> None:
        super().__init__('failed_airdrop_attempts', [])
        self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def airdrop_failed(self, eth_address, reason):
        with self._conn.cursor() as cur:
            cur.execute(f'''
                    INSERT INTO {self._table_name} (attempt_time, eth_address, reason)
                    VALUES (%s, %s, %s)
                ''',
                (datetime.now().timestamp(), eth_address, reason)
            )


class AirdropReadySet(BaseDB):
    def __init__(self):
        super().__init__('airdrop_ready', [])
        self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def register_airdrop(self, eth_address: str, airdrop_info: dict):
        finished = int(datetime.now().timestamp())
        duration = finished - airdrop_info['scheduled']
        with self._conn.cursor() as cur:
            cur.execute(f'''
                    INSERT INTO {self._table_name} (eth_address, scheduled_ts, finished_ts, duration, amount_galans)
                    VALUES (%s, %s, %s, %s, %s)
                ''',
                (eth_address, airdrop_info['scheduled'], finished, duration, airdrop_info['amount'])
            )

    def is_airdrop_ready(self, eth_address):
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {self._table_name} WHERE eth_address = %s", (eth_address,))
            return cur.fetchone() is not None


class GasTankTxInfo(NeonIndexedTxInfo):
    def __init__(self, tx_type: NeonIndexedTxInfo.Type, key: NeonIndexedTxInfo.Key, neon_tx: NeonTxInfo,
                 holder: str, iter_blocked_account: Iterator[str]):
        super().__init__(tx_type, key, neon_tx, holder, iter_blocked_account)
        self.iterations: Dict[int, int] = {}

    @staticmethod
    def create_tx_info(neon_tx_sig: str, message: bytes, tx_type: NeonIndexedTxInfo.Type,
                       key: NeonIndexedTxInfo.Key,
                       holder: str, iter_blocked_account: Iterator[str]) -> Optional[GasTankTxInfo]:
        neon_tx = NeonTxInfo.from_sig_data(message)
        if neon_tx.error:
            LOG.warning(f'Neon tx rlp error "{neon_tx.error}"')
            return None
        if neon_tx_sig != neon_tx.sig:
            LOG.warning(f'Neon tx hash {neon_tx.sig} != {neon_tx_sig}')
            return None

        return GasTankTxInfo(tx_type, key, neon_tx, holder, iter_blocked_account)

    def append_receipt(self, ix: SolNeonIxReceiptInfo):
        self.iterations[ix.neon_total_gas_used] = ix.neon_gas_used
        self.add_sol_neon_ix(ix)
        total_gas_used = ix.neon_total_gas_used
        for event in ix.neon_tx_event_list:
            self.add_neon_event(dataclasses.replace(
                event,
                total_gas_used=total_gas_used,
                sol_sig=ix.sol_sig,
                idx=ix.idx,
                inner_idx=ix.inner_idx
            ))
            total_gas_used += 1

        if ix.neon_tx_return is not None:
            self.neon_tx_res.set_res(status=ix.neon_tx_return.status, gas_used=ix.neon_tx_return.gas_used)
            self.neon_tx_res.set_sol_sig_info(ix.sol_sig, ix.idx, ix.inner_idx)
            self.add_neon_event(NeonLogTxEvent(
                event_type=NeonLogTxEvent.Type.Return,
                is_hidden=True, address=b'', topic_list=[],
                data=ix.neon_tx_return.status.to_bytes(1, 'little'),
                total_gas_used=ix.neon_tx_return.gas_used + 5000,
                sol_sig=ix.sol_sig, idx=ix.idx, inner_idx=ix.inner_idx
            ))
            self.set_status(GasTankTxInfo.Status.Done, ix.block_slot)

    def finalize(self):
        total_gas_used = 0
        for k, v in sorted(self.iterations.items()):
            if total_gas_used + v != k:
                raise Exception(f'{self.key} not all iterations were collected {sorted(self.iterations.items())}')
            total_gas_used += v

        self.complete_event_list()

    def iter_events(self) -> Iterator[Dict[str, Any]]:
        for ev in self.neon_tx_res.log_list:
            if not ev['neonIsHidden']:
                yield ev


# Interface class for work with the gas tank state from analyzers objects
class GasTankState:
    # Allow gas-less transactions for the specified address
    # There is no second allowance if one was allowed to this address.
    def allow_gas_less_tx(self, address: NeonAddress):
        pass


# Base class to create NeonEVM transaction analyzers for gas-tank
class GasTankNeonTxAnalyzer(abc.ABC):
    name = 'UNKNOWN'
    # Function to process NeonEVM transaction to find one that should be allowed with gas-less transactions
    # Arguments:
    #  - neon_tx - information about NeonEVM transaction
    #  - state - gas tank state
    @abc.abstractmethod
    def process(self, neon_tx: GasTankTxInfo, state: GasTankState) -> bool:
        pass


class GasTankSolTxAnalyzer(abc.ABC):
    name = 'UNKNOWN'

    @abc.abstractmethod
    def process(self, sol_tx: Dict[str, Any], state: GasTankState) -> bool:
        pass


class GasTank(IndexerBase, GasTankState):
    def __init__(self,
                 config: Config,
                 sol_tx_analyzer_list: List[GasTankSolTxAnalyzer],
                 neon_tx_analyzer_dict: Dict[NeonAddress, GasTankNeonTxAnalyzer],
                 faucet_url='',
                 max_conf=0.1):  # maximum confidence interval deviation related to price
        self._constants = SQLDict(tablename="constants")

        solana = SolInteractor(config, config.solana_url)
        last_known_slot = self._constants.get('latest_processed_slot', None)
        super().__init__(config, solana, last_known_slot)
        self.latest_processed_slot = self._start_slot
        self.current_slot = 0
        sol_tx_meta_dict = SolTxMetaDict()
        self._sol_tx_collector = FinalizedSolTxMetaCollector(config, self._solana, sol_tx_meta_dict, self._start_slot)

        self.evm_loader_id = str(self._config.evm_loader_id)

        # collection of eth-address-to-create-account-trx mappings
        # for every addresses that was already funded with airdrop
        self.airdrop_ready = AirdropReadySet()
        self.failed_attempts = FailedAttempts()
        self.airdrop_scheduled = SQLDict(tablename="airdrop_scheduled")

        self.faucet_url = faucet_url
        self.recent_price = None

        # It is possible to use different networks to obtain SOL price
        # but there will be different slot numbers so price should be updated every time
        self.always_reload_price = config.solana_url != config.pyth_solana_url
        self.pyth_client = PythNetworkClient(SolInteractor(config, config.pyth_solana_url))
        self.max_conf = Decimal(max_conf)
        self.session = requests.Session()

        self.sol_price_usd = None
        self.airdrop_amount_usd = None
        self.airdrop_amount_neon = None
        self.last_update_pyth_mapping = None
        self.max_update_pyth_mapping_int = 60 * 60  # update once an hour
        self.neon_large_tx_dict: Dict[str, NeonIndexedHolderInfo] = {}
        self.neon_processed_tx_dict: Dict[str, GasTankTxInfo] = {}
        self.last_finalized_slot: int = 0

        self._neon_tx_analyzer_dict = neon_tx_analyzer_dict
        self._sol_tx_analyzer_list = sol_tx_analyzer_list

    @staticmethod
    def get_current_time():
        return datetime.now().timestamp()

    def try_update_pyth_mapping(self):
        current_time = self.get_current_time()
        if self.last_update_pyth_mapping is None or abs(
            current_time - self.last_update_pyth_mapping) > self.max_update_pyth_mapping_int:
            try:
                self.pyth_client.update_mapping(self._config.pyth_mapping_account)
                self.last_update_pyth_mapping = current_time
            except BaseException as exc:
                LOG.error('Failed to update pyth.network mapping account data', exc_info=exc)
                return False

        return True

    def airdrop_to(self, eth_address, airdrop_galans):
        LOG.info(f"Airdrop {airdrop_galans} Galans to address: {eth_address}")
        json_data = {'wallet': eth_address, 'amount': airdrop_galans}
        resp = self.session.post(self.faucet_url + '/request_neon_in_galans', json=json_data)
        if not resp.ok:
            LOG.warning(f'Failed to airdrop: {resp.status_code}')
            return False

        return True

    # Method to process NeonEVM transaction extracted from the instructions
    def _process_neon_tx(self, tx_info: GasTankTxInfo) -> None:
        if tx_info.status != GasTankTxInfo.Status.Done or tx_info.neon_tx_res.status != '0x1':
            LOG.debug(f'SKIPPED {tx_info.key} status {tx_info.status} result {tx_info.neon_tx_res.status}: {tx_info}')
            return

        try:
            tx_info.finalize()
            tx = tx_info.neon_tx
            if tx.to_addr is None:
                return

            sender = tx.addr
            to = NeonAddress(tx.to_addr)
            LOG.debug(f'from: {sender}, to: {to}, callData: {tx.calldata}')

            neon_tx_analyzer = self._neon_tx_analyzer_dict.get(to, None)
            if neon_tx_analyzer is None:
                return

            LOG.debug(f'found analyzer {neon_tx_analyzer.name}')
            neon_tx_analyzer.process(tx_info, self)

        except Exception as error:
            LOG.warning(f'failed to analyze {tx_info.key}: {str(error)}')

    def _process_write_holder_ix(self, sol_neon_ix: SolNeonIxReceiptInfo) -> None:
        neon_tx_id = NeonIndexedHolderInfo.Key(sol_neon_ix.get_account(0), sol_neon_ix.neon_tx_sig)
        data = sol_neon_ix.ix_data[41:]
        chunk = NeonIndexedHolderInfo.DataChunk(
            offset=int.from_bytes(sol_neon_ix.ix_data[33:41], 'little'),
            length=len(data),
            data=data
        )
        neon_tx_data = self.neon_large_tx_dict.get(neon_tx_id.value, None)
        if neon_tx_data is None:
            LOG.debug(f'new NEON tx: {neon_tx_id} {len(chunk.data)} bytes at {chunk.offset}')
            neon_tx_data = NeonIndexedHolderInfo(neon_tx_id)
            self.neon_large_tx_dict[neon_tx_id.value] = neon_tx_data
        neon_tx_data.add_data_chunk(chunk)
        neon_tx_data.add_sol_neon_ix(sol_neon_ix)

    def _process_step_ix(self, sol_neon_ix: SolNeonIxReceiptInfo, ix_code: int) -> None:
        key = GasTankTxInfo.Key(sol_neon_ix)
        tx_info = self.neon_processed_tx_dict.get(key.value, None)
        if tx_info is not None:
            tx_info.append_receipt(sol_neon_ix)
            return

        neon_tx_id = NeonIndexedHolderInfo.Key(sol_neon_ix.get_account(0), sol_neon_ix.neon_tx_sig)
        neon_tx_data = self.neon_large_tx_dict.pop(neon_tx_id.value, None)
        if neon_tx_data is None:
            LOG.warning(f'holder account {neon_tx_id} is not in the collected data')
            return

        tx_type = GasTankTxInfo.Type(ix_code)
        first_blocked_account = 6
        tx_info = GasTankTxInfo.create_tx_info(
            sol_neon_ix.neon_tx_sig, neon_tx_data.data, tx_type, key,
            sol_neon_ix.get_account(0), sol_neon_ix.iter_account(first_blocked_account)
        )
        if tx_info is None:
            return
        tx_info.append_receipt(sol_neon_ix)

        if ix_code == EVM_LOADER_TRX_EXECUTE_FROM_ACCOUNT:
            if tx_info.status != GasTankTxInfo.Status.Done:
                LOG.warning(f'no tx_return for single call')
            else:
                self._process_neon_tx(tx_info)
        else:
            self.neon_processed_tx_dict[key.value] = tx_info

    def _process_call_raw_tx(self, sol_neon_ix: SolNeonIxReceiptInfo) -> None:
        if len(sol_neon_ix.ix_data) < 6:
            LOG.warning(f'no enough data to get Neon tx')
            return

        tx_info = GasTankTxInfo.create_tx_info(
            sol_neon_ix.neon_tx_sig, sol_neon_ix.ix_data[5:],
            GasTankTxInfo.Type.Single, GasTankTxInfo.Key(sol_neon_ix),
            '', iter(())
        )
        if tx_info is None:
            return
        tx_info.append_receipt(sol_neon_ix)

        if tx_info.status != GasTankTxInfo.Status.Done:
            LOG.warning(f'no tx_return for single call')
            return

        self._process_neon_tx(tx_info)

    def _process_call_raw_nochain_id_tx(self, sol_neon_ix: SolNeonIxReceiptInfo) -> None:
        key = GasTankTxInfo.Key(sol_neon_ix)
        tx_info = self.neon_processed_tx_dict.get(key.value, None)
        if tx_info is None:
            first_blocked_account = 6
            if len(sol_neon_ix.ix_data) < 14 or sol_neon_ix.account_cnt < first_blocked_account + 1:
                LOG.warning(f'no enough data or accounts to get Neon tx')
                return

            tx_info = GasTankTxInfo.create_tx_info(
                sol_neon_ix.neon_tx_sig, sol_neon_ix.ix_data[13:],
                GasTankTxInfo.Type.IterFromData, key,
                sol_neon_ix.get_account(0), sol_neon_ix.iter_account(first_blocked_account)
            )
            if tx_info is None:
                return
            self.neon_processed_tx_dict[key.value] = tx_info

        tx_info.append_receipt(sol_neon_ix)

    def _process_cancel(self, sol_neon_ix: SolNeonIxReceiptInfo) -> None:
        key = GasTankTxInfo.Key(sol_neon_ix)
        tx_info = self.neon_processed_tx_dict.get(key.value, None)
        if tx_info is None:
            LOG.warning(f'cancel unknown trx {key}')
            return
        tx_info.set_status(GasTankTxInfo.Status.Canceled, sol_neon_ix.block_slot)

    def _process_finalized_tx_list(self, block_slot: int) -> None:
        if self.last_finalized_slot >= block_slot:
            return

        self.last_finalized_slot = block_slot
        finalized_tx_list = [
            k for k, v in self.neon_processed_tx_dict.items() if
            v.status != GasTankTxInfo.Status.InProgress and v.last_block_slot < block_slot
        ]
        if not len(finalized_tx_list):
            return

        LOG.debug(f'finalized: {finalized_tx_list}')
        for k in finalized_tx_list:
            tx_info = self.neon_processed_tx_dict.pop(k)
            self._process_neon_tx(tx_info)

    # Method to process Solana transactions and extract NeonEVM transaction from the contract instructions.
    # For large NeonEVM transaction that passing to contract via account data, this method extracts and
    # combines chunk of data from different HolderWrite instructions. At any time `neon_large_tx`
    # dictionary contains actual NeonEVM transactions written into the holder accounts. The stored account
    # are cleared in case of execution, cancel trx or writing chunk of data from another NeonEVM transaction.
    # This logic are implemented according to the work with holder account inside contract.
    # Note: the `neon_large_tx` dictionary stored only in memory, so `last_processed_slot` move forward only
    # after finalize corresponding holder account. It is necessary for correct transaction processing after
    # restart the gas-tank service.
    # Note: this implementation analyzes only the final step in case of iterative execution. It simplifies it
    # but does not process events generated from the Solidity contract.
    def _process_neon_ix(self, tx: Dict[str, Any]):
        tx_receipt_info = SolTxReceiptInfo.from_tx_receipt(tx)

        self._process_finalized_tx_list(tx_receipt_info.block_slot)

        for sol_neon_ix in tx_receipt_info.iter_sol_neon_ix():
            ix_code = sol_neon_ix.ix_data[0]
            LOG.debug(f'instruction: {ix_code} {sol_neon_ix.neon_tx_sig}')
            LOG.debug(f'INSTRUCTION: {sol_neon_ix}')
            if ix_code == EVM_LOADER_HOLDER_WRITE:
                self._process_write_holder_ix(sol_neon_ix)

            elif ix_code in {EVM_LOADER_TRX_STEP_FROM_ACCOUNT,
                             EVM_LOADER_TRX_STEP_FROM_ACCOUNT_NO_CHAINID,
                             EVM_LOADER_TRX_EXECUTE_FROM_ACCOUNT}:
                self._process_step_ix(sol_neon_ix, ix_code)

            elif ix_code == EVM_LOADER_CALL_FROM_RAW_TRX:
                self._process_call_raw_tx(sol_neon_ix)

            elif ix_code == EVM_LOADER_STEP_FROM_RAW_TRX:
                self._process_call_raw_nochain_id_tx(sol_neon_ix)

            elif ix_code == EVM_LOADER_CANCEL:
                self._process_cancel(sol_neon_ix)

    def get_sol_usd_price(self):
        should_reload = self.always_reload_price
        if not should_reload:
            if self.recent_price is None or self.recent_price['valid_slot'] < self.current_slot:
                should_reload = True

        if should_reload:
            try:
                self.recent_price = self.pyth_client.get_price('Crypto.SOL/USD')
            except BaseException as exc:
                LOG.error('Exception occurred when reading price', exc_info=exc)
                return None

        return self.recent_price

    def get_airdrop_amount_galans(self):
        self.sol_price_usd = self.get_sol_usd_price()
        if self.sol_price_usd is None:
            LOG.warning("Failed to get SOL/USD price")
            return None

        neon_price_usd = self._config.neon_price_usd
        LOG.info(f"NEON price: ${neon_price_usd}")
        LOG.info(f"Price valid slot: {self.sol_price_usd['valid_slot']}")
        LOG.info(f"Price confidence interval: ${self.sol_price_usd['conf']}")
        LOG.info(f"SOL/USD = ${self.sol_price_usd['price']}")
        if self.sol_price_usd['conf'] / self.sol_price_usd['price'] > self.max_conf:
            LOG.warning(f"Confidence interval too large. Airdrops will deferred.")
            return None

        self.airdrop_amount_usd = AIRDROP_AMOUNT_SOL * self.sol_price_usd['price']
        self.airdrop_amount_neon = self.airdrop_amount_usd / neon_price_usd
        LOG.info(f"Airdrop amount: ${self.airdrop_amount_usd} ({self.airdrop_amount_neon} NEONs)\n")
        return int(self.airdrop_amount_neon * pow(Decimal(10), self._config.neon_decimals))

    def allow_gas_less_tx(self, account: NeonAddress) -> None:
        neon_address = str(account)
        if self.airdrop_ready.is_airdrop_ready(neon_address) or neon_address in self.airdrop_scheduled:
            # Target account already supplied with airdrop or airdrop already scheduled
            return
        LOG.info(f'Allow gas-less txs for {neon_address}')
        self.airdrop_scheduled[neon_address] = {'scheduled': self.get_current_time()}

    def process_scheduled_trxs(self):
        # Pyth.network mapping account was never updated
        if not self.try_update_pyth_mapping() and self.last_update_pyth_mapping is None:
            self.failed_attempts.airdrop_failed('ALL', 'mapping is empty')
            return

        airdrop_galans = self.get_airdrop_amount_galans()
        if airdrop_galans is None:
            LOG.warning('Failed to estimate airdrop amount. Defer scheduled airdrops.')
            self.failed_attempts.airdrop_failed('ALL', 'fail to estimate amount')
            return

        success_addresses = set()
        for eth_address, sched_info in self.airdrop_scheduled.items():
            if not self.airdrop_to(eth_address, airdrop_galans):
                self.failed_attempts.airdrop_failed(str(eth_address), 'airdrop failed')
                continue
            success_addresses.add(eth_address)
            self.airdrop_ready.register_airdrop(eth_address,
                                                {
                                                    'amount': airdrop_galans,
                                                    'scheduled': sched_info['scheduled']
                                                })

        for eth_address in success_addresses:
            if eth_address in self.airdrop_scheduled:
                del self.airdrop_scheduled[eth_address]

    def process_functions(self):
        """
        Overrides IndexerBase.process_functions
        """
        IndexerBase.process_functions(self)
        LOG.debug("Process receipts")
        self._process_receipts()
        self.process_scheduled_trxs()

    def _process_sol_tx(self, tx: Dict[str, Any]) -> bool:
        for sol_analyzer in self._sol_tx_analyzer_list:
            LOG.debug(f'trying: {sol_analyzer.name}...')
            if sol_analyzer.process(tx, self):
                LOG.debug(f'{sol_analyzer.name} success')
                return True
        return False

    def _process_receipts(self):
        last_block_slot = self._solana.get_block_slot(self._sol_tx_collector.commitment)
        for meta in self._sol_tx_collector.iter_tx_meta(last_block_slot, self._sol_tx_collector.last_block_slot):
            self.current_slot = meta.block_slot
            if check_error(meta.tx):
                continue

            if meta.tx['transaction']['message']['instructions'] is not None:
                with logging_context(sol_sig=meta.sol_sig):
                    if not self._process_sol_tx(meta.tx):
                        self._process_neon_ix(meta.tx)

        self.latest_processed_slot = self._sol_tx_collector.last_block_slot

        # Find the minimum start_block_slot through unfinished neon_large_tx. It is necessary for correct
        # transaction processing after restart the gas-tank service. See `_process_neon_ix`
        # for more information.
        outdated_holder_list = [
            tx.key for tx in self.neon_large_tx_dict.values()
            if tx.last_block_slot + self._config.holder_timeout < self._sol_tx_collector.last_block_slot
        ]
        for tx_key in outdated_holder_list:
            LOG.info(f"Outdated holder {tx_key}. Drop it.")
            self.neon_large_tx_dict.pop(tx_key)

        lost_tx_list = [
            k for k, v in self.neon_processed_tx_dict.items()
            if v.status == GasTankTxInfo.Status.InProgress
            and v.last_block_slot + self._config.holder_timeout < self._sol_tx_collector.last_block_slot
        ]
        for k in lost_tx_list:
            tx_info = self.neon_processed_tx_dict.pop(k)
            LOG.warning(f'Lost trx {tx_info.key}. Drop it.')

        for tx in self.neon_large_tx_dict.values():
            self.latest_processed_slot = min(self.latest_processed_slot, tx.start_block_slot - 1)
        for tx in self.neon_processed_tx_dict.values():
            self.latest_processed_slot = min(self.latest_processed_slot, tx.start_block_slot - 1)

        self._constants['latest_processed_slot'] = self.latest_processed_slot
        LOG.debug(
            f"Latest processed slot: {self.latest_processed_slot}, "
            f"Solana finalized slot {self._sol_tx_collector.last_block_slot}"
        )
