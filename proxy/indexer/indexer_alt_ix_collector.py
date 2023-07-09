import base58
import logging

from typing import List, Dict, Any

from ..common_neon.solana_tx import SolPubKey, SolCommit
from ..common_neon.layouts import ALTAccountInfo
from ..common_neon.solana_neon_tx_receipt import SolAltIxInfo, SolTxMetaInfo
from ..common_neon.constants import ADDRESS_LOOKUP_TABLE_ID
from ..common_neon.config import Config
from ..common_neon.solana_interactor import SolInteractor

from .indexed_objects import NeonIndexedBlockInfo, NeonIndexedAltInfo


LOG = logging.getLogger(__name__)


class AltIxCollector:
    def __init__(self, config: Config, solana: SolInteractor):
        self._config = config
        self._solana = solana

    def collect_in_block(self, neon_block: NeonIndexedBlockInfo) -> None:
        freeing_depth = self._config.alt_freeing_depth * 2
        check_slot = neon_block.block_slot - freeing_depth
        check_done_slot = neon_block.block_slot - 64
        if check_slot < 0:
            return

        for alt_info in list(neon_block.iter_alt_info()):
            if alt_info.block_slot > check_slot:
                continue
            elif alt_info.done_block_slot > check_done_slot:
                continue
            elif alt_info.done_block_slot > 0:
                pass
            elif not self._is_done_alt(neon_block, alt_info):
                continue
            else:
                # wait for transaction indexing
                alt_info.set_done_block_slot(neon_block.block_slot)
                continue

            alt_key = SolPubKey.from_string(alt_info.alt_key)
            sig_block_list = self._solana.get_sig_list_for_address(alt_key, None, 1000, SolCommit.Finalized)
            sig_list = [sig_block.get('signature', None) for sig_block in sig_block_list]
            tx_receipt_list = self._solana.get_tx_receipt_list(sig_list, SolCommit.Finalized)

            alt_ix_list = self._decode_alt_ixs(alt_info, tx_receipt_list)
            neon_block.done_alt_info(alt_info, alt_ix_list)

    def _is_done_alt(self, neon_block: NeonIndexedBlockInfo, alt_info: NeonIndexedAltInfo) -> bool:
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
        check_block_slot = neon_block.block_slot - self._config.alt_freeing_depth * 10
        if alt_info.block_slot < check_block_slot:
            return True
        return False

    @staticmethod
    def _decode_alt_ixs(alt_info: NeonIndexedAltInfo, tx_receipt_list: List[Dict[str, Any]]) -> List[SolAltIxInfo]:
        alt_program_key = str(ADDRESS_LOOKUP_TABLE_ID)
        alt_ix_list: List[SolAltIxInfo] = list()
        for tx_receipt in tx_receipt_list:
            if tx_receipt is None:
                continue

            has_alt_ix = False
            tx_meta = SolTxMetaInfo.from_tx_receipt(None, tx_receipt)
            for idx, ix in enumerate(tx_meta.ix_list):
                if not tx_meta.is_program(ix, alt_program_key):
                    continue

                try:
                    ix_data = base58.b58decode(ix.get('data', None))
                    ix_code = int.from_bytes(ix_data[:4], 'little')
                    has_alt_ix = True
                except BaseException as exc:
                    LOG.warning(
                        f'failed to decode ALT instruction data '
                        f'in Solana tx {tx_meta.sol_sig}:{tx_meta.block_slot}',
                        exc_info=exc
                    )
                    continue

                alt_ix_info = SolAltIxInfo.from_tx_meta(
                    tx_meta, idx, ix_code, alt_info.alt_key,
                    alt_info.neon_tx_sig
                )
                alt_ix_list.append(alt_ix_info)

            if not has_alt_ix:
                LOG.warning(f'ALT instruction does not exist in Solana tx {tx_meta.sol_sig}:{tx_meta.block_slot}')
        return alt_ix_list
