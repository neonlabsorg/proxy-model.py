from typing import Optional, List, Dict, Any, Tuple

from ..common_neon.utils import NeonTxReceiptInfo, SolBlockInfo
from ..common_neon.solana_neon_tx_receipt import SolNeonIxReceiptShortInfo, SolAltIxInfo

from .indexer_base import IndexerDBCtx
from .indexed_objects import NeonIndexedBlockInfo
from .neon_tx_logs_db import NeonTxLogsDB
from .neon_txs_db import NeonTxsDB
from .solana_blocks_db import SolBlocksDB
from .solana_neon_txs_db import SolNeonTxsDB
from .solana_tx_costs_db import SolTxCostsDB
from .stuck_neon_holders_db import StuckNeonHoldersDB
from .stuck_neon_txs_db import StuckNeonTxsDB
from .solana_alt_infos_db import SolAltInfosDB
from .solana_alt_txs_db import SolAltTxsDB
from .gas_less_usages_db import GasLessUsagesDB


class IndexerDB:
    def __init__(self, ctx: IndexerDBCtx):
        self._ctx = ctx
        self._sol_blocks_db = SolBlocksDB(self._ctx.db)
        self._sol_tx_costs_db = SolTxCostsDB(self._ctx.db)
        self._neon_txs_db = NeonTxsDB(self._ctx.db)
        self._sol_neon_txs_db = SolNeonTxsDB(self._ctx.db)
        self._neon_tx_logs_db = NeonTxLogsDB(self._ctx.db)
        self._gas_less_usages_db = GasLessUsagesDB(self._ctx.db)
        self._sol_alt_txs_db = SolAltTxsDB(self._ctx.db)
        self._stuck_neon_holders_db = StuckNeonHoldersDB(self._ctx.db)
        self._stuck_neon_txs_db = StuckNeonTxsDB(self._ctx.db)
        self._sol_alt_infos_db = SolAltInfosDB(self._ctx.db)

        self._finalized_db_list = [
            self._sol_blocks_db,
            self._sol_tx_costs_db,
            self._neon_txs_db,
            self._sol_neon_txs_db,
            self._neon_tx_logs_db,
        ]

        if self._ctx.is_reindexing_mode():
            # for indexing old blocks, init last slots from the start slot
            self._latest_slot = self._ctx.min_used_slot
            self._finalized_slot = self._ctx.min_used_slot
        else:
            self._latest_slot = self.latest_slot
            self._finalized_slot = self.finalized_slot

    def is_healthy(self) -> bool:
        return self._ctx.db.is_connected()

    def submit_block_list(self, min_used_slot: int, neon_block_queue: List[NeonIndexedBlockInfo]) -> None:
        self._ctx.db.run_tx(
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

        last_block = neon_block_queue[-1]
        if last_block.is_finalized:
            self._finalize_block_list(neon_block_queue)
        else:
            self._activate_block_list(neon_block_queue)

        self._set_latest_slot(last_block.block_slot)
        self._ctx.set_min_used_slot(min_used_slot)

        for block in neon_block_queue:
            block.mark_done()

    def _finalize_block_list(self, neon_block_queue: List[NeonIndexedBlockInfo]) -> None:
        block_slot_list = [
            block.block_slot
            for block in neon_block_queue
            if block.is_done and (block.block_slot > self._finalized_slot)
        ]
        if len(block_slot_list) == 0:
            return

        for db_table in self._finalized_db_list:
            db_table.finalize_block_list(self._finalized_slot, block_slot_list)

        last_block = neon_block_queue[-1]

        self._stuck_neon_holders_db.set_holder_list(
            last_block.stuck_block_slot,
            last_block.iter_stuck_neon_holder(self._ctx.config)
        )
        self._stuck_neon_txs_db.set_tx_list(
            True, last_block.stuck_block_slot,
            last_block.iter_stuck_neon_tx(self._ctx.config)
        )
        self._sol_alt_infos_db.set_alt_list(last_block.stuck_block_slot, last_block.iter_alt_info())

        self._set_finalized_slot(last_block.block_slot)

    def _activate_block_list(self, neon_block_queue: List[NeonIndexedBlockInfo]) -> None:
        last_block = neon_block_queue[-1]
        if not last_block.is_done:
            self._stuck_neon_txs_db.set_tx_list(
                False, last_block.block_slot,
                last_block.iter_stuck_neon_tx(self._ctx.config)
            )

        block_slot_list = [block.block_slot for block in neon_block_queue if not block.is_finalized]
        if not len(block_slot_list):
            return

        self._sol_blocks_db.activate_block_list(self._finalized_slot, block_slot_list)

    def _set_finalized_slot(self, slot: int) -> None:
        if self._finalized_slot >= slot:
            return

        self._finalized_slot = slot
        if not self._ctx.is_reindexing_mode():
            # don't change finalized slot for Proxy, because Indexer indexes old blocks
            self._ctx.constants_db['finalized_block_slot'] = slot

    def _set_latest_slot(self, slot: int) -> None:
        if self._latest_slot >= slot:
            return

        self._latest_slot = slot
        if not self._ctx.is_reindexing_mode():
            # don't change latest slot for Proxy, because Indexer indexes old blocks
            self._ctx.constants_db['latest_block_slot'] = slot

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
        return self._ctx.constants_db.get('starting_block_slot', 0)

    @property
    def latest_slot(self) -> int:
        return self._ctx.constants_db.get('latest_block_slot', 0)

    @property
    def finalized_slot(self) -> int:
        return self._ctx.constants_db.get('finalized_block_slot', 0)

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
