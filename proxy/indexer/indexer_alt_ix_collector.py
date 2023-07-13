from __future__ import annotations

import logging

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from ..common_neon.solana_tx import SolPubKey, SolCommit
from ..common_neon.layouts import ALTAccountInfo
from ..common_neon.solana_neon_tx_receipt import SolAltIxInfo, SolTxMetaInfo, SolIxMetaInfo
from ..common_neon.constants import ADDRESS_LOOKUP_TABLE_ID
from ..common_neon.config import Config
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.neon_instruction import AltIxCode

from .indexed_objects import NeonIndexedBlockInfo, NeonIndexedAltInfo


LOG = logging.getLogger(__name__)


class AltIxCollector:
    @dataclass(frozen=True)
    class _AltIxListResult:
        is_done: bool
        alt_ix_list: List[SolAltIxInfo]

    def __init__(self, config: Config, solana: SolInteractor):
        self._config = config
        self._solana = solana

    def collect_in_block(self, neon_block: NeonIndexedBlockInfo) -> None:
        fail_check_slot = neon_block.block_slot - self._config.alt_freeing_depth * 10
        next_check_slot = neon_block.block_slot + 64

        for alt_info in list(neon_block.iter_alt_info()):
            if alt_info.next_check_slot == 0:
                alt_info.set_next_check_slot(next_check_slot)
                continue
            elif alt_info.next_check_slot > neon_block.block_slot:
                continue
            alt_info.set_next_check_slot(next_check_slot)

            sig_block_list = self._solana.get_sig_list_for_address(alt_info.alt_key, None, 1000, SolCommit.Finalized)
            sig_list = [sig_block.get('signature', None) for sig_block in sig_block_list]
            tx_receipt_list = self._solana.get_tx_receipt_list(sig_list, SolCommit.Finalized)

            result = self._decode_alt_ix_list(alt_info, tx_receipt_list)
            if result.is_done:
                neon_block.done_alt_info(alt_info, result.alt_ix_list)
            elif (alt_info.block_slot < fail_check_slot) and self._is_done_alt(alt_info):
                neon_block.done_alt_info(alt_info, result.alt_ix_list)
            else:
                neon_block.add_alt_ix_list(alt_info, result.alt_ix_list)

    def _is_done_alt(self, alt_info: NeonIndexedAltInfo) -> bool:
        alt_address = SolPubKey.from_string(alt_info.alt_key)
        acct_info = self._solana.get_account_info(alt_address, commitment=SolCommit.Finalized)
        if acct_info is None:
            return True

        alt_acct_info = ALTAccountInfo.from_account_info(acct_info)
        if alt_acct_info is None:
            return True
        elif alt_acct_info.authority is None:
            LOG.warning(f'ALT {alt_info.alt_key} is frozen')
            return True

        if alt_acct_info.authority in self._config.operator_account_set:
            return False

        # don't wait for ALTs from other operators
        return True

    def _decode_alt_ix_list(self, alt_info: NeonIndexedAltInfo,
                            tx_receipt_list: List[Dict[str, Any]]) -> AltIxCollector._AltIxListResult:
        alt_ix_list: List[SolAltIxInfo] = list()
        is_done = False
        for tx_receipt in tx_receipt_list:
            if tx_receipt is None:
                LOG.warning(f'No transaction receipt for {str(alt_info)}')
                continue

            alt_ix_info = self._find_alt_ix(alt_info, tx_receipt)
            if alt_ix_info is None:
                continue

            if alt_ix_info.ix_code == AltIxCode.Freeze:
                LOG.warning(f'ALT {alt_info.alt_key} is frozen')
                is_done = True
            elif alt_ix_info.ix_code == AltIxCode.Close:
                is_done = True

            alt_ix_list.append(alt_ix_info)
        return self._AltIxListResult(is_done, alt_ix_list)

    def _find_alt_ix(self, alt_info: NeonIndexedAltInfo, tx_receipt: Dict[str, Any]) -> Optional[SolAltIxInfo]:
        tx_meta = SolTxMetaInfo.from_tx_receipt(None, tx_receipt)
        for ix_meta in tx_meta.ix_meta_list:
            if ix_meta.is_program(ADDRESS_LOOKUP_TABLE_ID):
                return self._decode_alt_ix(ix_meta, alt_info)

            for inner_ix_meta in tx_meta.inner_ix_meta_list(ix_meta):
                if inner_ix_meta.is_program(ADDRESS_LOOKUP_TABLE_ID):
                    return self._decode_alt_ix(inner_ix_meta, alt_info)

        LOG.warning(f'ALT instruction does not exist in Solana tx {str(tx_meta)}')
        return None

    @staticmethod
    def _decode_alt_ix(ix_meta: SolIxMetaInfo, alt_info: NeonIndexedAltInfo) -> Optional[SolAltIxInfo]:
        try:
            ix_data = ix_meta.ix_data
            ix_code = int.from_bytes(ix_data[:4], 'little')
        except BaseException as exc:
            LOG.warning(f'failed to decode ALT instruction data in Solana ix {str(ix_meta)}', exc_info=exc)
            return None

        return SolAltIxInfo.from_ix_meta(ix_meta, ix_code, alt_info.alt_key, alt_info.neon_tx_sig)
