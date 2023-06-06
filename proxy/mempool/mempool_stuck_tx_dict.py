
import logging

from typing import Dict, List, Optional

from .mempool_api import MPStuckTxInfo

from ..common_neon.utils.neon_tx_info import NeonTxInfo

LOG = logging.getLogger(__name__)


class MPStuckTxDict:
    def __init__(self):
        self._own_tx_dict: Dict[str, MPStuckTxInfo] = dict()
        self._external_tx_dict: Dict[str, MPStuckTxInfo] = dict()
        self._processed_tx_dict: Dict[str, MPStuckTxInfo] = dict()

    def add_external_tx_list(self, stuck_tx_list: List[MPStuckTxInfo]) -> None:
        tx_dict: Dict[str, MPStuckTxInfo] = dict()
        for stuck_tx in stuck_tx_list:
            neon_sig = stuck_tx.neon_tx.sig
            if neon_sig in self._own_tx_dict:
                continue
            elif neon_sig in self._processed_tx_dict:
                continue
            elif neon_sig not in self._external_tx_dict:
                LOG.debug(f'found external stuck tx {stuck_tx.account}: {stuck_tx.neon_tx}')

            tx_dict[neon_sig] = stuck_tx
        self._external_tx_dict = tx_dict

    def add_own_tx(self, stuck_tx: MPStuckTxInfo) -> None:
        neon_sig = stuck_tx.neon_tx.sig
        if neon_sig in self._processed_tx_dict:
            return
        elif neon_sig in self._own_tx_dict:
            return

        LOG.debug(f'found own stuck tx {stuck_tx.account}: {stuck_tx.neon_tx}')

        self._external_tx_dict.pop(neon_sig, None)
        self._own_tx_dict[neon_sig] = stuck_tx

    def acquire_tx(self) -> Optional[MPStuckTxInfo]:
        if len(self._own_tx_dict):
            neon_sig, stuck_tx = self._own_tx_dict.popitem()
            self._processed_tx_dict[neon_sig] = stuck_tx
            LOG.debug(f'get own stuck tx for processing: {stuck_tx.account}: {stuck_tx.neon_tx}')
            return stuck_tx

        elif len(self._external_tx_dict):
            neon_sig, stuck_tx = self._own_tx_dict.popitem()
            self._processed_tx_dict[neon_sig] = stuck_tx

            LOG.debug(f'get external stuck tx for processing: {stuck_tx.account}: {stuck_tx.neon_tx}')
            return stuck_tx

        return None

    def done_tx(self, neon_sig: str) -> None:
        stuck_tx = self._processed_tx_dict.pop(neon_sig, None)
        if stuck_tx is not None:
            LOG.debug(f'done stuck tx {stuck_tx.account}: {stuck_tx.neon_tx}')

    def get(self, neon_sig: str) -> Optional[NeonTxInfo]:
        return (
            self._own_tx_dict.get(neon_sig, None) or
            self._external_tx_dict.get(neon_sig, None) or
            self._processed_tx_dict.get(neon_sig, None)
        )
