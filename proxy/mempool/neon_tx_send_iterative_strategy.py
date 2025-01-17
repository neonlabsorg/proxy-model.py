import logging

from typing import List, Generator

from ..common_neon.errors import NoMoreRetriesError
from ..common_neon.solana_tx import SolTx
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.solana_tx_list_sender import SolTxSendState
from ..common_neon.neon_tx_result_info import NeonTxResultInfo
from ..common_neon.neon_instruction import EvmIxCodeName, EvmIxCode
from ..common_neon.evm_config import EVMConfig

from .neon_tx_sender_ctx import NeonTxSendCtx
from .neon_tx_send_base_strategy import BaseNeonTxStrategy
from .neon_tx_send_strategy_alt_stage import alt_strategy
from .neon_tx_send_strategy_newaccount_stage import NewAccountNeonTxPrepStage


LOG = logging.getLogger(__name__)


class IterativeNeonTxStrategy(BaseNeonTxStrategy):
    name = EvmIxCodeName().get(EvmIxCode.TxStepFromData)
    _cancel_name = EvmIxCodeName().get(EvmIxCode.CancelWithHash)

    def __init__(self, ctx: NeonTxSendCtx) -> None:
        super().__init__(ctx)
        # EVM steps is valid only for iterative transactions
        self._evm_step_cnt = EVMConfig().neon_evm_steps
        # Apply priority fee only in iterative transactions
        self._cu_priority_fee = ctx.config.cu_priority_fee
        self._prep_stage_list.append(NewAccountNeonTxPrepStage(ctx))

    def complete_init(self) -> None:
        super().complete_init()
        self._ctx.mark_resource_use()

    def execute(self) -> NeonTxResultInfo:
        assert self.is_valid()

        self._sol_tx_list_sender.clear()

        if (not self._ctx.is_stuck_tx()) and (not self._ctx.has_sol_tx(self.name)):
            self._raise_if_blocked_account()

        if not self._recheck_tx_list([self.name]):
            if not self._ctx.is_stuck_tx():
                self._send_tx_list(self._build_execute_tx_list())

        # Not enough iterations, try `retry_on_fail` times to complete the Neon Tx
        retry_on_fail = self._ctx.config.retry_on_fail
        for retry in range(retry_on_fail):
            neon_tx_res = self._decode_neon_tx_result()
            if neon_tx_res.is_valid():
                return neon_tx_res

            self._send_tx_list(self._build_complete_tx_list())

        raise NoMoreRetriesError()

    def cancel(self) -> None:
        if not self._recheck_tx_list([self._cancel_name]):
            self._send_tx_list(self._build_cancel_tx_list())

    def _build_execute_tx_list(self) -> Generator[List[SolTx], None, None]:
        yield from self._build_tx_list_impl(self._ctx.iter_cnt)

    def _build_complete_tx_list(self) -> Generator[List[SolTx], None, None]:
        LOG.debug('No receipt -> execute additional iteration')
        yield from self._build_tx_list_impl(1)

    def _build_tx_list_impl(self, iter_cnt: int) -> Generator[List[SolTx], None, None]:
        tx_list: List[SolTx] = [self._build_tx() for _ in range(iter_cnt)]

        LOG.debug(
            f'Total iterations: {len(tx_list)}, '
            f'total EVM steps: {self._ctx.emulated_evm_step_cnt}, '
            f'EVM steps per iteration: {self._evm_step_cnt}'
        )
        yield tx_list

    def _validate(self) -> bool:
        return (
            self._validate_stuck_tx() and
            self._validate_tx_has_chainid()
        )

    def _build_tx(self) -> SolLegacyTx:
        uniq_idx = self._ctx.sol_tx_cnt
        builder = self._ctx.ix_builder
        return self._build_cu_tx(builder.make_tx_step_from_data_ix(self._evm_step_cnt, uniq_idx))

    def _build_cancel_tx(self) -> SolLegacyTx:
        return self._build_cu_tx(name='CancelWithHash', ix=self._ctx.ix_builder.make_cancel_ix())

    def _decode_neon_tx_result(self) -> NeonTxResultInfo:
        neon_tx_res = NeonTxResultInfo()
        tx_send_state_list = self._sol_tx_list_sender.tx_state_list
        neon_total_gas_used = 0
        has_already_finalized = False
        status = SolTxSendState.Status

        for tx_send_state in tx_send_state_list:
            if tx_send_state.status == status.AlreadyFinalizedError:
                has_already_finalized = True
                continue
            elif tx_send_state.status != status.GoodReceipt:
                continue

            sol_neon_ix = self._find_sol_neon_ix(tx_send_state)
            if sol_neon_ix is None:
                continue

            neon_total_gas_used = max(neon_total_gas_used, sol_neon_ix.neon_total_gas_used)

            ret = sol_neon_ix.neon_tx_return
            if ret is None:
                continue

            neon_tx_res.set_res(status=ret.status, gas_used=ret.gas_used)
            LOG.debug(f'Set Neon tx result: {neon_tx_res}')
            return neon_tx_res

        if has_already_finalized:
            neon_tx_res.set_lost_res(neon_total_gas_used)
            LOG.debug(f'Set lost Neon tx result: {neon_tx_res}')

        return neon_tx_res

    def _build_cancel_tx_list(self) -> Generator[List[SolTx], None, None]:
        yield [self._build_cancel_tx()]


@alt_strategy
class ALTIterativeNeonTxStrategy(IterativeNeonTxStrategy):
    pass
