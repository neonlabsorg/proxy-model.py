from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple

from .gas_less_usages_db import GasLessUsagesDB
from .indexed_objects import NeonIndexedBlockInfo
from .neon_tx_logs_db import NeonTxLogsDB
from .neon_txs_db import NeonTxsDB
from .solana_alt_infos_db import SolAltInfosDB
from .solana_alt_txs_db import SolAltTxsDB
from .solana_blocks_db import SolBlocksDB
from .solana_neon_txs_db import SolNeonTxsDB
from .solana_tx_costs_db import SolTxCostsDB
from .stuck_neon_holders_db import StuckNeonHoldersDB
from .stuck_neon_txs_db import StuckNeonTxsDB

from ..common_neon.config import Config
from ..common_neon.db.constats_db import ConstantsDB
from ..common_neon.db.db_connect import DBConnection
from ..common_neon.solana_neon_tx_receipt import SolNeonIxReceiptShortInfo, SolAltIxInfo
from ..common_neon.utils import NeonTxReceiptInfo, SolBlockInfo


class IndexerDB:
    base_start_slot_name = 'start_block_slot'

    def __init__(self, config: Config, db_conn: DBConnection, reindex_ident: str):
        self._config = config
        self._db_conn = db_conn
        self._reindex_ident = reindex_ident + '-' if len(reindex_ident) else ''

        self._start_slot_name = self._reindex_ident + self.base_start_slot_name
        self._stop_slot_name = self._reindex_ident + 'stop_block_slot'
        self._min_used_slot_name = self._reindex_ident + 'min_receipt_block_slot'

        self._constants_db = ConstantsDB(db_conn)
        self._sol_blocks_db = SolBlocksDB(db_conn)
        self._sol_tx_costs_db = SolTxCostsDB(db_conn)
        self._neon_txs_db = NeonTxsDB(db_conn)
        self._sol_neon_txs_db = SolNeonTxsDB(db_conn)
        self._neon_tx_logs_db = NeonTxLogsDB(db_conn)
        self._gas_less_usages_db = GasLessUsagesDB(db_conn)
        self._sol_alt_txs_db = SolAltTxsDB(db_conn)
        self._stuck_neon_holders_db = StuckNeonHoldersDB(db_conn)
        self._stuck_neon_txs_db = StuckNeonTxsDB(db_conn)
        self._sol_alt_infos_db = SolAltInfosDB(db_conn)

        self._finalized_db_list = [
            self._sol_blocks_db,
            self._sol_tx_costs_db,
            self._neon_txs_db,
            self._sol_neon_txs_db,
            self._neon_tx_logs_db,
        ]

        self._latest_slot = self.latest_slot
        self._finalized_slot = self.finalized_slot

        self._start_slot = 0
        self._stop_slot: Optional[int] = None
        self._min_used_slot = 0

    @staticmethod
    def from_db(config: Config, db: DBConnection, reindex_ident: str = '') -> IndexerDB:
        db = IndexerDB(config, db, reindex_ident)

        db._min_used_slot = db._constants_db.get(db._min_used_slot_name, 0)
        db._start_slot = db._constants_db.get(db._start_slot_name, db._min_used_slot)
        db._stop_slot = db._constants_db.get(db._stop_slot_name, None)

        return db

    @staticmethod
    def from_range(config: Config, db: DBConnection, start_slot: int,
                   reindex_ident: str = '', stop_slot: Optional[int] = None) -> IndexerDB:
        db = IndexerDB(config, db, reindex_ident)

        db._start_slot = start_slot
        db._min_used_slot = start_slot
        db._stop_slot = stop_slot

        db._min_used_slot = db._constants_db[db._min_used_slot_name] = start_slot

        if db.is_reindexing_mode():
            db._constants_db[db._start_slot_name] = start_slot
            db._constants_db[db._stop_slot_name] = stop_slot

        return db

    @property
    def reindex_ident(self) -> str:
        return self._reindex_ident

    @property
    def start_slot(self) -> int:
        return self._start_slot

    @property
    def stop_slot(self) -> Optional[int]:
        return self._stop_slot

    def is_reindexing_mode(self) -> bool:
        return len(self._reindex_ident) > 0

    def is_healthy(self) -> bool:
        return self._db_conn.is_connected()

    def drop_not_finalized_history(self) -> None:
        assert not self.is_reindexing_mode()
        self._db_conn.run_tx(
            lambda: self._drop_not_finalized_history()
        )

    def _drop_not_finalized_history(self) -> None:
        for db_table in self._finalized_db_list:
            db_table.finalize_block_list(self._finalized_slot, self._latest_slot, [])

    def submit_block_list(self, min_used_slot: int, neon_block_queue: List[NeonIndexedBlockInfo]) -> None:
        self._db_conn.run_tx(
            lambda: self._submit_block_list(min_used_slot, neon_block_queue)
        )

    def _submit_block_list(self, min_used_slot: int, neon_block_queue: List[NeonIndexedBlockInfo]) -> None:
        new_neon_block_queue = [block for block in neon_block_queue if not block.is_done]

        if len(new_neon_block_queue) > 0:
            self._sol_blocks_db.set_block_list(new_neon_block_queue)
            self._neon_txs_db.set_tx_list(new_neon_block_queue)
            self._neon_tx_logs_db.set_tx_list(new_neon_block_queue)
            self._sol_neon_txs_db.set_tx_list(new_neon_block_queue)
            self._sol_alt_txs_db.set_tx_list(new_neon_block_queue)
            self._sol_tx_costs_db.set_cost_list(new_neon_block_queue)
            self._gas_less_usages_db.set_tx_list(new_neon_block_queue)

        self._set_min_used_slot(min_used_slot)

        if not self.is_reindexing_mode():
            last_block = neon_block_queue[-1]
            self._set_latest_slot(last_block.block_slot)
            if last_block.is_finalized:
                self._finalize_block_list(neon_block_queue)
            else:
                self._activate_block_list(neon_block_queue)

        for block in neon_block_queue:
            block.mark_done()

    def _finalize_block_list(self, neon_block_queue: List[NeonIndexedBlockInfo]) -> None:
        assert not self.is_reindexing_mode()

        block_slot_list = [
            block.block_slot
            for block in neon_block_queue
            if block.is_done and (block.block_slot > self._finalized_slot)
        ]
        if len(block_slot_list) == 0:
            return

        last_block = neon_block_queue[-1]

        for db_table in self._finalized_db_list:
            db_table.finalize_block_list(self._finalized_slot, last_block.block_slot, block_slot_list)

        self._stuck_neon_holders_db.set_holder_list(
            last_block.stuck_block_slot,
            last_block.iter_stuck_neon_holder(self._config)
        )
        self._stuck_neon_txs_db.set_tx_list(
            True, last_block.stuck_block_slot,
            last_block.iter_stuck_neon_tx(self._config)
        )
        self._sol_alt_infos_db.set_alt_list(last_block.stuck_block_slot, last_block.iter_alt_info())

        self._set_finalized_slot(last_block.block_slot)

    def _activate_block_list(self, neon_block_queue: List[NeonIndexedBlockInfo]) -> None:
        assert not self.is_reindexing_mode()

        last_block = neon_block_queue[-1]
        if not last_block.is_done:
            self._stuck_neon_txs_db.set_tx_list(
                False, last_block.block_slot,
                last_block.iter_stuck_neon_tx(self._config)
            )

        block_slot_list = [block.block_slot for block in neon_block_queue if not block.is_finalized]
        if not len(block_slot_list):
            return

        self._sol_blocks_db.activate_block_list(self._finalized_slot, block_slot_list)

    def _set_finalized_slot(self, slot: int) -> None:
        assert not self.is_reindexing_mode()

        if self._finalized_slot >= slot:
            return

        self._finalized_slot = slot
        self._constants_db['finalized_block_slot'] = slot

    def _set_latest_slot(self, slot: int) -> None:
        assert not self.is_reindexing_mode()

        if self._latest_slot >= slot:
            return

        self._latest_slot = slot
        self._constants_db['latest_block_slot'] = slot

    def _set_min_used_slot(self, slot: int) -> None:
        if self._min_used_slot >= slot:
            return

        self._min_used_slot = slot
        self._constants_db[self._min_used_slot_name] = slot

    def set_start_slot(self, slot: int) -> None:
        if self._start_slot >= slot:
            return

        self._set_min_used_slot(slot)

        self._start_slot = slot
        if self.is_reindexing_mode():
            self._constants_db[self._start_slot_name] = slot

    def done(self) -> None:
        assert self.is_reindexing_mode()

        for k in [self._start_slot_name, self._stop_slot_name, self._min_used_slot_name]:
            if k in self._constants_db:
                del self._constants_db[k]

    def get_block_by_slot(self, block_slot: int) -> SolBlockInfo:
        return self._get_block_by_slot(block_slot, self.earliest_slot, self.latest_slot)

    def _get_block_by_slot(self, block_slot: int, starting_block_slot: int, latest_block_slot: int) -> SolBlockInfo:
        if starting_block_slot <= block_slot <= latest_block_slot:
            return self._sol_blocks_db.get_block_by_slot(block_slot, latest_block_slot)
        return SolBlockInfo(block_slot=0)

    def get_block_by_hash(self, block_hash: str) -> SolBlockInfo:
        return self._sol_blocks_db.get_block_by_hash(block_hash, self.latest_slot)

    @property
    def earliest_slot(self) -> int:
        return self._constants_db.get('starting_block_slot', 0)

    @property
    def latest_slot(self) -> int:
        assert not self.is_reindexing_mode()
        return self._constants_db.get('latest_block_slot', 0)

    @property
    def finalized_slot(self) -> int:
        return self._constants_db.get('finalized_block_slot', 0)

    @property
    def min_used_slot(self) -> int:
        return self._min_used_slot

    @property
    def earliest_block(self) -> SolBlockInfo:
        slot = self.earliest_slot
        latest_slot = self.latest_slot
        return self._get_block_by_slot(slot, slot, latest_slot)

    @property
    def latest_block(self) -> SolBlockInfo:
        earliest_slot = self.earliest_slot
        slot = self.latest_slot
        return self._get_block_by_slot(slot, earliest_slot, slot)

    @property
    def finalized_block(self) -> SolBlockInfo:
        earliest_slot = self.earliest_slot
        slot = self.finalized_slot
        return self._get_block_by_slot(slot, earliest_slot, slot)

    def get_log_list(self, from_block: Optional[int], to_block: Optional[int],
                     address_list: List[str], topic_list: List[List[str]]) -> List[Dict[str, Any]]:
        return self._neon_tx_logs_db.get_log_list(from_block, to_block, address_list, topic_list)

    def get_tx_list_by_block_slot(self, block_slot: int) -> List[NeonTxReceiptInfo]:
        return self._neon_txs_db.get_tx_list_by_block_slot(block_slot)

    def get_tx_by_neon_sig(self, neon_sig: str) -> Optional[NeonTxReceiptInfo]:
        return self._neon_txs_db.get_tx_by_neon_sig(neon_sig)

    def get_tx_by_sender_nonce(self, sender: str, tx_nonce: int) -> Optional[NeonTxReceiptInfo]:
        return self._neon_txs_db.get_tx_by_sender_nonce(sender, tx_nonce)

    def get_tx_by_block_slot_tx_idx(self, block_slot: int, tx_idx: int) -> Optional[NeonTxReceiptInfo]:
        return self._neon_txs_db.get_tx_by_block_slot_tx_idx(block_slot, tx_idx)

    def get_sol_sig_list_by_neon_sig(self, neon_sig: str) -> List[str]:
        return self._sol_neon_txs_db.get_sol_sig_list_by_neon_sig(neon_sig)

    def get_alt_sig_list_by_neon_sig(self, neon_sig: str) -> List[str]:
        return self._sol_alt_txs_db.get_alt_sig_list_by_neon_sig(neon_sig)

    def get_sol_ix_info_list_by_neon_sig(self, neon_sig: str) -> List[SolNeonIxReceiptShortInfo]:
        return self._sol_neon_txs_db.get_sol_ix_info_list_by_neon_sig(neon_sig)

    def get_sol_alt_tx_list_by_neon_sig(self, neon_sig: str) -> List[SolAltIxInfo]:
        return self._sol_alt_txs_db.get_alt_ix_list_by_neon_sig(neon_sig)

    def get_stuck_neon_holder_list(self, block_slot: int) -> Tuple[Optional[int], List[Dict[str, Any]]]:
        return self._stuck_neon_holders_db.get_holder_list(block_slot)

    def get_stuck_neon_tx_list(self, is_finalized: bool, block_slot: int) -> Tuple[Optional[int], List[Dict[str, Any]]]:
        return self._stuck_neon_txs_db.get_tx_list(is_finalized, block_slot)

    def get_sol_alt_info_list(self, block_slot: int) -> Tuple[Optional[int], List[Dict[str, Any]]]:
        return self._sol_alt_infos_db.get_alt_list(block_slot)
