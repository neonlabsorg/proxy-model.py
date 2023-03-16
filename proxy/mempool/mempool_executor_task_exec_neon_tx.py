import logging

from ..common_neon.elf_params import ElfParams
from ..common_neon.errors import BadResourceError
from ..common_neon.errors import BlockedAccountsError, NodeBehindError, SolanaUnavailableError, NonceTooLowError

from ..mempool.mempool_api import MPTxExecRequest, MPTxExecResult, MPTxExecResultCode
from ..mempool.mempool_executor_task_base import MPExecutorBaseTask
from ..mempool.neon_tx_sender import NeonTxSendStrategyExecutor
from ..mempool.neon_tx_sender_ctx import NeonTxSendCtx
from ..mempool.operator_resource_mng import OpResInfo


LOG = logging.getLogger(__name__)


class MPExecutorExecNeonTxTask(MPExecutorBaseTask):
    def execute_neon_tx(self, mp_tx_req: MPTxExecRequest):
        neon_tx_exec_cfg = mp_tx_req.neon_tx_exec_cfg
        try:
            assert neon_tx_exec_cfg is not None
            self.execute_neon_tx_impl(mp_tx_req)
        except BlockedAccountsError:
            LOG.debug(f"Failed to execute tx {mp_tx_req.sig}, got blocked accounts result")
            return MPTxExecResult(MPTxExecResultCode.Reschedule, neon_tx_exec_cfg)
        except (NodeBehindError, SolanaUnavailableError) as exc:
            LOG.warning(f'Failed to execute tx {mp_tx_req.sig}, got solana error', exc_info=exc)
            return MPTxExecResult(MPTxExecResultCode.Reschedule, neon_tx_exec_cfg)
        except BadResourceError as exc:
            # TODO: move to rescrudule result
            LOG.debug(f'Failed to execute tx {mp_tx_req.sig}, got bad resource error: {str(exc)}')
            return MPTxExecResult(MPTxExecResultCode.BadResource, neon_tx_exec_cfg)
        except BaseException as exc:
            if isinstance(exc, NonceTooLowError):
                # sender is absent on the level of SolTxSender
                exc = NonceTooLowError(mp_tx_req.sender_address, exc.tx_nonce, exc.state_tx_cnt)
            LOG.error(f'Failed to execute tx {mp_tx_req.sig}', exc_info=exc)
            return MPTxExecResult(MPTxExecResultCode.Failed, exc)
        return MPTxExecResult(MPTxExecResultCode.Done, neon_tx_exec_cfg)

    def execute_neon_tx_impl(self, mp_tx_req: MPTxExecRequest):
        ElfParams().set_elf_param_dict(mp_tx_req.elf_param_dict)

        resource = OpResInfo.from_ident(mp_tx_req.res_ident)

        neon_tx = mp_tx_req.neon_tx
        neon_tx_exec_cfg = mp_tx_req.neon_tx_exec_cfg
        strategy_ctx = NeonTxSendCtx(self._config, self._solana, resource, neon_tx, neon_tx_exec_cfg)
        strategy_executor = NeonTxSendStrategyExecutor(strategy_ctx)
        strategy_executor.execute()
