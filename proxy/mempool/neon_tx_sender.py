import logging

from ..common_neon.address import NeonAddress
from ..common_neon.data import NeonEmulatedResult
from ..common_neon.emulator_interactor import call_tx_emulated
from ..common_neon.errors import NonceTooLowError, WrongStrategyError
from ..common_neon.errors import NoMoreRetriesError
from ..common_neon.utils import NeonTxResultInfo

from .neon_tx_send_base_strategy import BaseNeonTxStrategy
from .neon_tx_send_holder_strategy import HolderNeonTxStrategy, ALTHolderNeonTxStrategy
from .neon_tx_send_simple_holder_strategy import SimpleHolderNeonTxStrategy, ALTSimpleHolderNeonTxStrategy
from .neon_tx_send_iterative_strategy import IterativeNeonTxStrategy, ALTIterativeNeonTxStrategy
from .neon_tx_send_nochainid_strategy import NoChainIdNeonTxStrategy, ALTNoChainIdNeonTxStrategy
from .neon_tx_send_simple_strategy import SimpleNeonTxStrategy, ALTSimpleNeonTxStrategy
from .neon_tx_sender_ctx import NeonTxSendCtx


LOG = logging.getLogger(__name__)


class NeonTxSendStrategyExecutor:
    _strategy_list = [
        SimpleNeonTxStrategy, ALTSimpleNeonTxStrategy,
        IterativeNeonTxStrategy, ALTIterativeNeonTxStrategy,
        SimpleHolderNeonTxStrategy, ALTSimpleHolderNeonTxStrategy,
        HolderNeonTxStrategy, ALTHolderNeonTxStrategy,
        NoChainIdNeonTxStrategy, ALTNoChainIdNeonTxStrategy
    ]

    def __init__(self, ctx: NeonTxSendCtx):
        self._ctx = ctx

    def execute(self) -> NeonTxResultInfo:
        self._validate_nonce()
        return self._execute()

    def _init_state_tx_cnt(self) -> None:
        neon_account_info = self._ctx.solana.get_neon_account_info(NeonAddress(self._ctx.sender))
        state_tx_cnt = neon_account_info.tx_count if neon_account_info is not None else 0
        self._ctx.set_state_tx_cnt(state_tx_cnt)

    def _emulate_neon_tx(self) -> None:
        emulated_result: NeonEmulatedResult = call_tx_emulated(self._ctx.config, self._ctx.neon_tx)
        self._ctx.set_emulated_result(emulated_result)
        self._validate_nonce()

    def _validate_nonce(self) -> None:
        self._init_state_tx_cnt()
        if self._ctx.state_tx_cnt > self._ctx.neon_tx.nonce:
            raise NonceTooLowError(self._ctx.sender, self._ctx.neon_tx.nonce, self._ctx.state_tx_cnt)

    def _execute(self) -> NeonTxResultInfo:
        start = self._ctx.strategy_idx
        end = len(self._strategy_list)
        for strategy_idx in range(start, end):
            Strategy = self._strategy_list[strategy_idx]
            try:
                strategy: BaseNeonTxStrategy = Strategy(self._ctx)
                if not strategy.validate():
                    LOG.debug(f'Skip strategy {Strategy.name}: {strategy.validation_error_msg}')
                    continue

                LOG.debug(f'Use strategy {Strategy.name}')
                strategy.start()
                self._ctx.set_strategy_idx(strategy_idx)

                # Try `retry_on_fail` times to prepare Neon tx for execution
                retry_on_fail = self._ctx.config.retry_on_fail
                for retry in range(retry_on_fail):
                    has_changes = strategy.prep_before_emulate()
                    if has_changes or (retry == 0):
                        self._emulate_neon_tx()
                        strategy.update_after_emulate()
                    if has_changes:
                        continue

                    return strategy.execute()

                # Can't prepare Neon tx for execution in `retry_on_fail` attempts (each call of preparing returned True)
                raise NoMoreRetriesError()

            except WrongStrategyError:
                continue
            except (BaseException,):
                raise
            finally:
                self._init_state_tx_cnt()

        raise RuntimeError('transaction is too big for execution')
