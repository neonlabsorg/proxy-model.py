from __future__ import annotations

import solders.transaction
import solders.message

from .solana_tx import SolTx, SolAccount, SolSig


SolLegacyMsg = solders.message.Message
SolLegacyLowLevelTx = solders.transaction.Transaction

_SolTxError = solders.transaction.TransactionError


class SolLegacyTx(SolTx):
    """Legacy transaction class to represent an atomic versioned transaction."""

    @property
    def low_level_tx(self) -> SolLegacyLowLevelTx:
        return self._solders_legacy_tx

    @property
    def message(self) -> SolLegacyMsg:
        return self._solders_legacy_tx.message

    def _serialize(self) -> bytes:
        if not self._verify_sign_list():
            raise AttributeError('Transaction has not been signed correctly')

        return bytes(self._solders_legacy_tx)

    def _verify_sign_list(self) -> bool:
        try:
            self._solders_legacy_tx.verify()
        except _SolTxError:
            return False
        return True

    def _sig(self) -> SolSig:
        return self._solders_legacy_tx.signatures[0]

    def _sign(self, *signer: SolAccount) -> None:
        self._solders_legacy_tx.sign(signer, self._solders_legacy_tx.message.recent_blockhash)

    def _clone(self) -> SolLegacyTx:
        return SolLegacyTx(self.name, self._decode_ix_list())
