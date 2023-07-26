from __future__ import annotations

import logging
import time

from typing import List, Optional, Dict, Type

from .indexed_objects import NeonIndexedBlockInfo, NeonIndexedBlockDict, SolNeonDecoderCtx, SolNeonDecoderStat
from .indexer_base import IndexerDBCtx
from .indexer_db import IndexerDB
from .neon_ix_decoder import DummyIxDecoder, get_neon_ix_decoder_list
from .neon_ix_decoder_deprecate import get_neon_ix_decoder_deprecated_list
from .tracer_api_client import TracerAPIClient
from .solana_block_net_cache import SolBlockNetCache
from .indexer_validate_stuck_objs import StuckObjectValidator
from .indexer_alt_ix_collector import AltIxCollector

from ..common_neon.metrics_logger import MetricsLogger
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.solana_tx import SolCommit
from ..common_neon.solana_tx_error_parser import SolTxErrorParser
from ..common_neon.utils.json_logger import logging_context
from ..common_neon.utils.solana_block import SolBlockInfo
from ..common_neon.errors import SolHistoryNotFound

from ..statistic.data import NeonBlockStatData
from ..statistic.indexer_client import IndexerStatClient


LOG = logging.getLogger(__name__)


class Indexer:
    def __init__(self, ctx: IndexerDBCtx):
        self._ctx = ctx
        self._db = IndexerDB(ctx)
        self._solana = SolInteractor(ctx.config, ctx.config.solana_url)

        self._tracer_api = TracerAPIClient(ctx.config)

        self._counted_logger = MetricsLogger(ctx.config)
        self._stat_client = IndexerStatClient(ctx.config)
        self._stat_client.start()
        self._last_stat_time = 0.0

        self._confirmed_slot: Optional[int] = None

        self._last_processed_slot = 0
        self._last_confirmed_slot = 0
        self._last_finalized_slot = 0
        self._last_tracer_slot: Optional[int] = None
        self._neon_block_dict = NeonIndexedBlockDict()

        self._stuck_obj_validator = StuckObjectValidator(ctx.config, self._solana)
        self._alt_ix_collector = AltIxCollector(ctx.config, self._solana)
        self._sol_block_net_cache = SolBlockNetCache(ctx.config, self._solana)

        self._decoder_stat = SolNeonDecoderStat()

        sol_neon_ix_decoder_list: List[Type[DummyIxDecoder]] = list()
        sol_neon_ix_decoder_list.extend(get_neon_ix_decoder_list())
        sol_neon_ix_decoder_list.extend(get_neon_ix_decoder_deprecated_list())

        self._sol_neon_ix_decoder_dict: Dict[int, Type[DummyIxDecoder]] = dict()
        for decoder in sol_neon_ix_decoder_list:
            ix_code = decoder.ix_code()
            assert ix_code not in self._sol_neon_ix_decoder_dict
            self._sol_neon_ix_decoder_dict[ix_code] = decoder

    def _save_checkpoint(self, dctx: SolNeonDecoderCtx) -> None:
        if dctx.is_neon_block_queue_empty():
            return

        neon_block_queue = dctx.neon_block_queue
        neon_block = neon_block_queue[-1]
        self._alt_ix_collector.collect_in_block(neon_block)

        # validate stuck objects only on the last confirmed block
        if not neon_block.is_finalized:
            self._stuck_obj_validator.validate_block(neon_block)
        else:
            self._neon_block_dict.finalize_neon_block(neon_block)
            self._sol_block_net_cache.finalize_block(neon_block.sol_block)

        cache_stat = self._neon_block_dict.stat
        self._db.submit_block_list(cache_stat.min_block_slot, neon_block_queue)
        dctx.clear_neon_block_queue()

    def _complete_neon_block(self, dctx: SolNeonDecoderCtx) -> None:
        if not dctx.has_neon_block():
            return

        is_finalized = dctx.is_finalized()
        neon_block = dctx.neon_block
        if is_finalized:
            neon_block.mark_finalized()

        if not neon_block.is_completed:
            neon_block.complete_block()
            self._neon_block_dict.add_neon_block(neon_block)
            self._print_stat(dctx)
        dctx.complete_neon_block()

        # in confirmed mode: collect all blocks
        # in finalized mode: collect block by batches
        if is_finalized and dctx.is_neon_block_queue_full():
            self._save_checkpoint(dctx)

        self._commit_stat(neon_block)

    def _commit_stat(self, neon_block: NeonIndexedBlockInfo):
        if not self._ctx.config.gather_statistics:
            return

        if neon_block.is_finalized:
            for tx_stat in neon_block.iter_stat_neon_tx(self._ctx.config):
                self._stat_client.commit_neon_tx_result(tx_stat)

        block_stat = NeonBlockStatData(
            reindex_ident=self._ctx.prefix,
            start_block=self._ctx.start_slot,
            parsed_block=neon_block.block_slot,
            finalized_block=self._last_finalized_slot,
            confirmed_block=self._last_confirmed_slot,
            tracer_block=self._last_tracer_slot
        )
        self._stat_client.commit_block_stat(block_stat)

        now = time.time()
        if abs(now - self._last_stat_time) < 1:
            return

        self._last_stat_time = now
        self._stat_client.commit_db_health(self._db.is_healthy())
        self._stat_client.commit_solana_rpc_health(self._solana.is_healthy())

    def _new_neon_block(self, dctx: SolNeonDecoderCtx, sol_block: SolBlockInfo) -> NeonIndexedBlockInfo:
        if not dctx.is_finalized():
            return NeonIndexedBlockInfo(sol_block)

        stuck_slot = sol_block.block_slot
        holder_slot, neon_holder_list = self._db.get_stuck_neon_holder_list(sol_block.block_slot)
        tx_slot, neon_tx_list = self._db.get_stuck_neon_tx_list(True, sol_block.block_slot)
        _, alt_info_list = self._db.get_sol_alt_info_list(sol_block.block_slot)

        if (holder_slot is not None) and (tx_slot is not None) and (holder_slot != tx_slot):
            LOG.warning(f'Holder stuck block {holder_slot} != tx stuck block {tx_slot}')
            neon_holder_list.clear()
            neon_tx_list.clear()
            alt_info_list.clear()

        elif tx_slot is not None:
            stuck_slot = tx_slot

        elif holder_slot is not None:
            stuck_slot = holder_slot

        return NeonIndexedBlockInfo.from_stuck_data(
            sol_block, stuck_slot,
            neon_holder_list, neon_tx_list, alt_info_list
        )

    def _locate_neon_block(self, dctx: SolNeonDecoderCtx, sol_block: SolBlockInfo) -> NeonIndexedBlockInfo:
        self._last_processed_slot = sol_block.block_slot

        # The same block
        if dctx.has_neon_block() and (dctx.neon_block.block_slot == sol_block.block_slot):
            return dctx.neon_block

        neon_block = self._neon_block_dict.find_neon_block(sol_block.block_slot)
        if neon_block:
            pass
        elif dctx.has_neon_block():
            neon_block = NeonIndexedBlockInfo.from_block(dctx.neon_block, sol_block)
        else:
            neon_block = self._new_neon_block(dctx, sol_block)

        # The next step, the indexer chooses the next block and saves of the current block in DB, cache ...
        self._complete_neon_block(dctx)
        dctx.set_neon_block(neon_block)
        return neon_block

    def _collect_neon_txs(self, dctx: SolNeonDecoderCtx, stop_slot: int, sol_commit: SolCommit.Type) -> None:
        start_slot = self._ctx.start_slot
        root_neon_block = self._neon_block_dict.finalized_neon_block
        if root_neon_block:
            start_slot = root_neon_block.block_slot

        if self._last_tracer_slot is not None:
            stop_slot = min(stop_slot, self._last_tracer_slot)
        if stop_slot < start_slot:
            return
        dctx.set_slot_range(start_slot, stop_slot, sol_commit)

        for sol_block in self._sol_block_net_cache.iter_block(dctx):
            neon_block = self._locate_neon_block(dctx, sol_block)
            if neon_block.is_completed:
                continue

            for sol_tx_meta in dctx.iter_sol_tx_meta(sol_block):
                sol_tx_cost = dctx.sol_tx_cost
                neon_block.add_sol_tx_cost(sol_tx_cost)
                is_error = SolTxErrorParser(self._ctx.config.evm_program_id, sol_tx_meta.tx).check_if_error()

                for sol_neon_ix in dctx.iter_sol_neon_ix():
                    with logging_context(sol_neon_ix=sol_neon_ix.req_id):
                        SolNeonIxDecoder = self._sol_neon_ix_decoder_dict.get(sol_neon_ix.ix_code, DummyIxDecoder)
                        sol_neon_ix_decoder = SolNeonIxDecoder(dctx)
                        if sol_neon_ix_decoder.is_stuck():
                            continue

                        neon_block.add_sol_neon_ix(sol_neon_ix)
                        if is_error:
                            sol_neon_ix_decoder.decode_failed_neon_tx_event_list()
                            # LOG.debug('failed tx')
                            continue
                        sol_neon_ix_decoder.execute()

        with logging_context(sol_neon_ix=f'end-{dctx.sol_commit[:3]}-{dctx.stop_slot}'):
            self._complete_neon_block(dctx)
            self._save_checkpoint(dctx)

    def _has_new_blocks(self) -> bool:
        if self._ctx.is_reindexing_mode():
            finalized_slot = self._solana.get_block_slot(SolCommit.Finalized)
            finalized_slot = min(self._ctx.stop_slot, finalized_slot)
            result = self._last_finalized_slot < finalized_slot
            self._last_finalized_slot = finalized_slot
        else:
            self._last_confirmed_slot = self._solana.get_block_slot(SolCommit.Confirmed)
            result = (self._confirmed_slot or 1) != self._last_confirmed_slot
            if result:
                self._last_finalized_slot = self._solana.get_block_slot(SolCommit.Finalized)
                self._last_tracer_slot = self._tracer_api.max_slot()
        return result

    def run(self):
        if self._ctx.is_reindexing_mode():
            with logging_context(reindex_ident=self._ctx.prefix):
                self._run()
        else:
            self._run()

    def _run(self):
        check_sec = float(self._ctx.config.indexer_check_msec) / 1000
        while self._is_done_parsing():
            if self._has_new_blocks():
                continue

            self._decoder_stat.start_timer()
            try:
                self._process_solana_blocks()

            except BaseException as exc:
                LOG.warning('Exception on transactions processing', exc_info=exc)

            finally:
                self._decoder_stat.commit_timer()

            time.sleep(check_sec)

    def _is_done_parsing(self) -> bool:
        if not self._ctx.is_reindexing_mode():
            return False
        return self._ctx.stop_slot <= self._last_processed_slot

    def _process_solana_blocks(self) -> None:
        dctx = SolNeonDecoderCtx(self._ctx.config, self._decoder_stat)
        try:
            self._collect_neon_txs(dctx, self._last_finalized_slot, SolCommit.Finalized)

        except SolHistoryNotFound as err:
            first_slot = self._check_first_slot()
            LOG.debug(
                f'first slot: {first_slot}, '
                f'start slot: {dctx.start_slot}, '
                f'stop slot: {dctx.stop_slot}, '
                f'skip parsing of finalized history: {str(err)}'
            )
            return

        # Don't parse not-finalized blocks on reindexing of old blocks
        if self._ctx.is_reindexing_mode():
            return

        # If there were a lot of transactions in the finalized state,
        # the head of finalized blocks will go forward
        # and there are no reason to parse confirmed blocks,
        # because on next iteration there will be the next portion of finalized blocks
        finalized_block_slot = self._solana.get_block_slot(SolCommit.Finalized)
        if (finalized_block_slot - dctx.stop_slot) >= 3:
            LOG.debug(f'skip parsing of not-finalized history: {finalized_block_slot} > {dctx.stop_slot}')
            return

        try:
            self._collect_neon_txs(dctx, self._last_confirmed_slot, SolCommit.Confirmed)

            # Save confirmed block only after successfully parsing,
            #  otherwise try to parse blocks again
            self._confirmed_slot = self._last_confirmed_slot

        except SolHistoryNotFound as err:
            LOG.debug(f'skip parsing of not-finalized history: {str(err)}')

    def _check_first_slot(self) -> int:
        first_slot = self._solana.get_first_available_block()
        start_slot = first_slot + 512
        if self._ctx.start_slot < start_slot:
            self._ctx.set_start_slot(start_slot)

        # Skip history if it was cleaned by the Solana Node
        finalized_neon_block = self._neon_block_dict.finalized_neon_block
        if (finalized_neon_block is not None) and (start_slot > finalized_neon_block.block_slot):
            self._neon_block_dict.clear()
        return first_slot

    def _print_stat(self, dctx: SolNeonDecoderCtx) -> None:
        cache_stat = self._neon_block_dict.stat
        latest_value_dict = dict()

        if self._counted_logger.is_print_time():
            state_stat = dctx.stat
            latest_value_dict = {
                'start block slot': self._ctx.start_slot,
                'current block slot': self._last_processed_slot,
                'min used block slot': cache_stat.min_block_slot,

                'processing ms': state_stat.processing_time_ms,
                'processed solana blocks': state_stat.sol_block_cnt,
                'corrupted neon blocks': state_stat.neon_corrupted_block_cnt,
                'processed solana transactions': state_stat.sol_tx_meta_cnt,
                'processed neon instructions': state_stat.sol_neon_ix_cnt,
            }

            if not self._ctx.is_reindexing_mode():
                latest_value_dict.update({
                    'tracer block slot': self._last_tracer_slot,
                    'confirmed block slot': self._last_confirmed_slot,
                    'finalized block slot': self._last_finalized_slot,
                })
            else:
                latest_value_dict.update({
                    'stop block slot': self._ctx.stop_slot,
                })
            state_stat.reset()

        with logging_context(ident='stat'):
            self._counted_logger.print(
                list_value_dict={
                    'neon blocks': cache_stat.neon_block_cnt,
                    'neon holders': cache_stat.neon_holder_cnt,
                    'neon transactions': cache_stat.neon_tx_cnt,
                    'solana instructions': cache_stat.sol_neon_ix_cnt,
                    'solana alt infos': cache_stat.sol_alt_info_cnt,
                },
                latest_value_dict=latest_value_dict
            )
