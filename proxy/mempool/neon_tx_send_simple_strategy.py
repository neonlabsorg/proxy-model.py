import logging

from ..common_neon.errors import CUBudgetExceededError
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.solana_tx_list_sender import SolTxListSender
from ..common_neon.utils import NeonTxResultInfo

from ..mempool.neon_tx_send_base_strategy import BaseNeonTxStrategy
from ..mempool.neon_tx_send_strategy_base_stages import alt_strategy
from ..mempool.neon_tx_sender_ctx import NeonTxSendCtx


LOG = logging.getLogger(__name__)


class SimpleNeonTxStrategy(BaseNeonTxStrategy):
    name = 'TxExecFromData'

    def __init__(self, ctx: NeonTxSendCtx):
        super().__init__(ctx)

    def _validate(self) -> bool:
        return (
            # self._validate_notdeploy_tx() and
            # self._validate_evm_step_cnt() and  <- by default, try to execute the neon tx in one solana tx
            self._validate_tx_has_chainid() and
            self._validate_no_resize_iter_cnt()
        )

    def _validate_evm_step_cnt(self) -> bool:
        if self._ctx.emulated_evm_step_cnt < self._evm_step_cnt:
            return True
        self._validation_error_msg = 'Too lot of EVM steps'
        return False

    def _validate_no_resize_iter_cnt(self) -> bool:
        if self._ctx.resize_iter_cnt <= 0:
            return True
        self._validation_error_msg = 'Has account resize iterations'
        return False

    def _build_tx(self) -> SolLegacyTx:
        return self._build_cu_tx(self._ctx.ix_builder.make_tx_exec_from_data_ix())

    def execute(self) -> NeonTxResultInfo:
        assert self.is_valid()

        tx_sender = SolTxListSender(self._ctx.config, self._ctx.solana, self._ctx.signer)
        tx_send_state_list = tx_sender.send([self._build_tx()])
        neon_tx_res = self._decode_neon_tx_result(tx_send_state_list, False)

        if not neon_tx_res.is_valid():
            raise CUBudgetExceededError()
        return neon_tx_res


@alt_strategy
class ALTSimpleNeonTxStrategy(SimpleNeonTxStrategy):
    pass
