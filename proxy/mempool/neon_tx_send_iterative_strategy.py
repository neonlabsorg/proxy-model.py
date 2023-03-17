import logging

from typing import List, Generator

from ..common_neon.errors import NoMoreRetriesError, WrongNumberOfItersError
from ..common_neon.solana_tx import SolTx
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.solana_tx_list_sender import SolTxSendState
from ..common_neon.utils import NeonTxResultInfo
from ..common_neon.solana_tx_error_parser import SolTxError

from ..mempool.neon_tx_send_base_strategy import BaseNeonTxStrategy
from ..mempool.neon_tx_send_strategy_base_stages import alt_strategy
from ..mempool.neon_tx_sender_ctx import NeonTxSendCtx


LOG = logging.getLogger(__name__)


class IterativeNeonTxStrategy(BaseNeonTxStrategy):
    name = 'TxStepFromData'
    _cancel_name = 'CancelWithHash'

    def __init__(self, ctx: NeonTxSendCtx) -> None:
        super().__init__(ctx)
        self._uniq_idx = 0

    def execute(self) -> NeonTxResultInfo:
        assert self.is_valid()

        neon_tx_res = self._execute()
        if not neon_tx_res.is_valid():
            neon_tx_res = self._cancel()

        return neon_tx_res

    def _execute(self) -> NeonTxResultInfo:
        self._send_tx_list([self.name], self._build_full_tx_list())
        neon_tx_res = self._decode_neon_tx_result()
        if neon_tx_res.is_valid():
            return neon_tx_res

        # There is no receipt -> tx isn't completed -> try to complete tx in n retries
        tx_list_sender = self._sol_tx_list_sender
        retry_on_fail = self._ctx.config.retry_on_fail
        for retry in range(retry_on_fail):
            if not tx_list_sender.has_good_receipt_list():
                break

            self._send_tx_list([], self._build_complete_tx_list())
            neon_tx_res = self._decode_neon_tx_result()
            if neon_tx_res.is_valid():
                return neon_tx_res

        raise NoMoreRetriesError()

    def _cancel(self) -> NeonTxResultInfo:
        self._send_tx_list([self._cancel_name], self._build_cancel_tx_list())

        tx_send_state_list = self._sol_tx_list_sender.tx_state_list
        if len(tx_send_state_list) != 1:
            raise WrongNumberOfItersError()

        tx_state = tx_send_state_list[0]
        status = SolTxSendState.Status
        if tx_state.status not in {status.GoodReceipt, status.LogTruncatedError}:
            raise SolTxError(tx_state.receipt)

        sol_neon_ix = self._find_sol_neon_ix(tx_state)
        neon_total_gas_used = max(sol_neon_ix.neon_total_gas_used, 1)

        neon_tx_res = NeonTxResultInfo()
        neon_tx_res.set_canceled_result(neon_total_gas_used)
        LOG.debug(f'Set canceled Neon tx result: {neon_tx_res}')

        return neon_tx_res

    def _validate(self) -> bool:
        return self._validate_tx_has_chainid()

    def _build_tx(self) -> SolLegacyTx:
        self._uniq_idx += 1
        return self._build_cu_tx(self._ctx.ix_builder.make_tx_step_from_data_ix(self._evm_step_cnt, self._uniq_idx))

    def _decode_neon_tx_result(self) -> NeonTxResultInfo:
        tx_send_state_list = self._sol_tx_list_sender.tx_state_list
        neon_tx_res = NeonTxResultInfo()
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

            neon_tx_res.set_result(status=ret.status, gas_used=ret.gas_used)
            LOG.debug(f'Set Neon tx result: {neon_tx_res}')
            return neon_tx_res

        if has_already_finalized:
            neon_tx_res.set_lost_result(neon_total_gas_used)
            LOG.debug(f'Set lost Neon tx result: {neon_tx_res}')

        return neon_tx_res

    def _build_full_tx_list(self) -> Generator[List[SolTx], None, None]:
        LOG.debug(
            f'Total EVM steps {self._ctx.emulated_evm_step_cnt}, '
            f'total resize iterations {self._ctx.resize_iter_cnt}'
        )

        emulated_step_cnt = max(self._ctx.emulated_evm_step_cnt, self._evm_step_cnt)
        additional_iter_cnt = self._ctx.resize_iter_cnt
        additional_iter_cnt += 2  # begin + finalization

        yield from self._build_tx_list(emulated_step_cnt, additional_iter_cnt)

    def _build_complete_tx_list(self) -> Generator[List[SolTx], None, None]:
        LOG.debug('No receipt -> execute additional iteration')
        yield from self._build_tx_list(0, 1)

    def _build_tx_list(self, total_evm_step_cnt: int, additional_iter_cnt: int) -> Generator[List[SolTx], None, None]:
        tx_list: List[SolTx] = list()
        save_evm_step_cnt = total_evm_step_cnt

        for _ in range(additional_iter_cnt):
            tx_list.append(self._build_tx())

        while total_evm_step_cnt > 0:
            total_evm_step_cnt -= self._evm_step_cnt
            tx_list.append(self._build_tx())

        LOG.debug(f'Total iterations {len(tx_list)} for {save_evm_step_cnt} ({self._evm_step_cnt}) EVM steps')
        yield tx_list

    def _build_cancel_tx_list(self) -> Generator[List[SolTx], None, None]:
        yield [self._build_cu_tx(name='CancelWithHash', ix=self._ctx.ix_builder.make_cancel_ix())]


@alt_strategy
class ALTIterativeNeonTxStrategy(IterativeNeonTxStrategy):
    pass
