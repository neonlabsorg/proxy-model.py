from __future__ import annotations

import logging
from typing import List

from ..common_neon.errors import NoMoreRetriesError
from ..common_neon.solana_tx import SolTx
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.solana_tx_list_sender import SolTxListSender
from ..common_neon.utils import NeonTxResultInfo

from ..mempool.neon_tx_send_base_strategy import BaseNeonTxStrategy
from ..mempool.neon_tx_send_strategy_base_stages import alt_strategy
from ..mempool.neon_tx_sender_ctx import NeonTxSendCtx


LOG = logging.getLogger(__name__)


class IterativeNeonTxStrategy(BaseNeonTxStrategy):
    name = 'TxStepFromData'

    def __init__(self, ctx: NeonTxSendCtx) -> None:
        super().__init__(ctx)
        self._uniq_idx = 0

    def _validate(self) -> bool:
        return self._validate_tx_has_chainid()

    def _build_tx(self) -> SolLegacyTx:
        self._uniq_idx += 1
        return self._build_cu_tx(self._ctx.ix_builder.make_tx_step_from_data_ix(self._evm_step_cnt, self._uniq_idx))

    def _build_tx_list(self, total_evm_step_cnt: int, additional_iter_cnt: int) -> List[SolTx]:
        tx_list: List[SolTx] = []
        save_evm_step_cnt = total_evm_step_cnt

        for _ in range(additional_iter_cnt):
            tx_list.append(self._build_tx())

        while total_evm_step_cnt > 0:
            total_evm_step_cnt -= self._evm_step_cnt
            tx_list.append(self._build_tx())

        LOG.debug(f'Total iterations {len(tx_list)} for {save_evm_step_cnt} ({self._evm_step_cnt}) EVM steps')
        return tx_list

    def _execute(self) -> NeonTxResultInfo:
        tx_sender = SolTxListSender(self._ctx.config, self._ctx.solana, self._ctx.signer)

        LOG.debug(
            f'Total EVM steps {self._ctx.emulated_evm_step_cnt}, '
            f'total resize iterations {self._ctx.resize_iter_cnt}'
        )

        emulated_step_cnt = max(self._ctx.emulated_evm_step_cnt, self._evm_step_cnt)
        additional_iter_cnt = self._ctx.resize_iter_cnt
        additional_iter_cnt += 2  # begin + finalization
        tx_list = self._build_tx_list(emulated_step_cnt, additional_iter_cnt)

        # Try to complete tx in n retries
        retry_on_fail = self._ctx.config.retry_on_fail
        for retry in range(retry_on_fail):
            tx_send_state_list = tx_sender.send(tx_list)
            neon_tx_res = self._decode_neon_tx_result(tx_send_state_list, False)
            if neon_tx_res.is_valid():
                return neon_tx_res
            elif tx_sender.has_good_receipt_list():
                LOG.debug('No receipt -> execute additional iteration')
                tx_list = self._build_tx_list(0, 1)
            else:
                break

        raise NoMoreRetriesError()

    def _cancel(self) -> NeonTxResultInfo:
        tx_sender = SolTxListSender(self._ctx.config, self._ctx.solana, self._ctx.signer)
        tx_send_state_list = tx_sender.send([self._build_cancel_tx()])
        return self._decode_neon_tx_result(tx_send_state_list, True)

    def execute(self) -> NeonTxResultInfo:
        assert self.is_valid()

        neon_tx_res = self._execute()

        if not neon_tx_res.is_valid():
            neon_tx_res = self._cancel()

        if not neon_tx_res.is_valid():
            raise NoMoreRetriesError()
        return neon_tx_res


@alt_strategy
class ALTIterativeNeonTxStrategy(IterativeNeonTxStrategy):
    pass
