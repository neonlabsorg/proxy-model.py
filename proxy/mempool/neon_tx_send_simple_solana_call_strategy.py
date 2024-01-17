from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.neon_instruction import EvmIxCode, EvmIxCodeName

from .neon_tx_send_simple_strategy import SimpleNeonTxStrategy
from .neon_tx_send_strategy_alt_stage import alt_strategy


class SimpleNeonTxSolanaCallStrategy(SimpleNeonTxStrategy):
    name = EvmIxCodeName().get(EvmIxCode.TxExecFromDataSolanaCall)

    def _validate(self) -> bool:
        return (
            self._validate_stuck_tx() and
            self._validate_tx_has_chainid() and 
            self._ctx.has_external_solana_call()
        )

    def _build_tx(self) -> SolLegacyTx:
        return self._build_cu_tx(self._ctx.ix_builder.make_tx_exec_from_data_solana_call_ix())


@alt_strategy
class ALTSimpleNeonTxSolanaCallStrategy(SimpleNeonTxSolanaCallStrategy):
    pass
