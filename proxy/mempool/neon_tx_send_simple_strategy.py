import logging

from typing import List

from ..common_neon.errors import NoMoreRetriesError
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.solana_tx_list_sender import SolTxListSender, SolTxSendState
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
            self._validate_tx_has_chainid() and
            self._validate_no_resize_iter_cnt()
        )

    def _validate_no_resize_iter_cnt(self) -> bool:
        if self._ctx.resize_iter_cnt <= 0:
            return True
        self._validation_error_msg = 'Has account resize iterations'
        return False

    def _build_tx(self) -> SolLegacyTx:
        return self._build_cu_tx(self._ctx.ix_builder.make_tx_exec_from_data_ix())

    def _decode_neon_tx_result(self, tx_send_state_list: List[SolTxSendState]) -> NeonTxResultInfo:
        neon_tx_res = NeonTxResultInfo()
        if len(tx_send_state_list) == 0:
            return neon_tx_res

        s = SolTxSendState.Status
        tx_state = tx_send_state_list[0]
        if tx_state.status == s.GoodReceipt:
            sol_neon_ix = self._find_sol_neon_ix(tx_state)
            ret = sol_neon_ix.neon_tx_return
            if ret is not None:
                neon_tx_res.set_result(status=ret.status, gas_used=ret.gas_used)
                LOG.debug(f'Set Neon tx result: {neon_tx_res}')

        elif tx_state.status == s.LogTruncatedError:
            neon_tx_res.set_lost_result(gas_used=1)  # unknown gas
            LOG.debug(f'Set truncated Neon tx result: {neon_tx_res}')

        return neon_tx_res

    def execute(self) -> NeonTxResultInfo:
        assert self.is_valid()

        tx_sender = SolTxListSender(self._ctx.config, self._ctx.solana, self._ctx.signer)
        tx_send_state_list = tx_sender.send([self._build_tx()])
        neon_tx_res = self._decode_neon_tx_result(tx_send_state_list)

        if not neon_tx_res.is_valid():
            raise NoMoreRetriesError()

        return neon_tx_res


@alt_strategy
class ALTSimpleNeonTxStrategy(SimpleNeonTxStrategy):
    pass
