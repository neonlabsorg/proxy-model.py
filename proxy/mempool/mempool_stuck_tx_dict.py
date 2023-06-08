
import logging

from typing import Dict, List, Optional

from .mempool_api import MPStuckTxInfo
from .mempool_neon_tx_dict import MPTxDict

LOG = logging.getLogger(__name__)


class MPStuckTxDict:
    def __init__(self, completed_tx_dict: MPTxDict):
        self._completed_tx_dict = completed_tx_dict
        self._own_tx_dict: Dict[str, MPStuckTxInfo] = dict()
        self._external_tx_dict: Dict[str, MPStuckTxInfo] = dict()
        self._processed_tx_dict: Dict[str, MPStuckTxInfo] = dict()

    def add_external_tx_list(self, stuck_tx_list: List[MPStuckTxInfo]) -> None:
        tx_dict: Dict[str, MPStuckTxInfo] = dict()
        for stuck_tx in stuck_tx_list:
            neon_sig = stuck_tx.sig
            if neon_sig in self._own_tx_dict:
                continue
            elif neon_sig in self._processed_tx_dict:
                continue
            elif neon_sig in self._completed_tx_dict:
                continue
            elif neon_sig not in self._external_tx_dict:
                LOG.debug(f'found external stuck tx {stuck_tx.account}: {stuck_tx.neon_tx}')

            tx_dict[neon_sig] = stuck_tx
        self._external_tx_dict = tx_dict

    def add_own_tx(self, stuck_tx: MPStuckTxInfo) -> None:
        neon_sig = stuck_tx.sig
        if neon_sig in self._processed_tx_dict:
            return
        elif neon_sig in self._own_tx_dict:
            return
        elif neon_sig in self._completed_tx_dict:
            return

        LOG.debug(f'found own stuck tx {str(stuck_tx)}')

        self._external_tx_dict.pop(neon_sig, None)
        self._own_tx_dict[neon_sig] = stuck_tx

    def peek_tx(self) -> Optional[MPStuckTxInfo]:
        for stuck_tx in self._own_tx_dict.values():
            return stuck_tx

        for stuck_tx in self._external_tx_dict.values():
            return stuck_tx

        return None

    def acquire_tx(self, tx: MPStuckTxInfo) -> Optional[MPStuckTxInfo]:
        neon_sig = tx.neon_tx.sig
        stuck_tx = (
            self._own_tx_dict.pop(neon_sig, None) or
            self._external_tx_dict.pop(neon_sig, None)
        )
        assert stuck_tx is not None

        self._processed_tx_dict[neon_sig] = stuck_tx
        LOG.debug(f'start processing of stuck tx {str(stuck_tx)}')
        return stuck_tx

    def skip_tx(self, stuck_tx: MPStuckTxInfo) -> None:
        neon_sig = stuck_tx.sig
        if self._own_tx_dict.pop(neon_sig, None):
            pass
        elif self._external_tx_dict.pop(neon_sig, None):
            pass
        elif self._processed_tx_dict.pop(neon_sig, None):
            pass
        else:
            assert False, f'{neon_sig} not found in the list of stuck txs'
        LOG.debug(f'skip stuck tx {str(stuck_tx)}')

    def done_tx(self, neon_sig: str) -> None:
        stuck_tx = self._processed_tx_dict.pop(neon_sig)
        LOG.debug(f'done stuck tx {str(stuck_tx)}')
