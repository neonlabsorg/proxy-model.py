import abc
import logging

from typing import Optional, List, cast

from ..common_neon.elf_params import ElfParams
from ..common_neon.solana_neon_tx_receipt import SolTxReceiptInfo
from ..common_neon.solana_tx import SolBlockHash, SolTx, SolTxIx
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.solana_tx_list_sender import SolTxListSender, SolTxSendState
from ..common_neon.utils import NeonTxResultInfo

from ..mempool.neon_tx_sender_ctx import NeonTxSendCtx

LOG = logging.getLogger(__name__)


class BaseNeonTxPrepStage(abc.ABC):
    def __init__(self, ctx: NeonTxSendCtx):
        self._ctx = ctx

    @abc.abstractmethod
    def build_prep_tx_list_before_emulate(self) -> List[List[SolTx]]:
        pass

    @abc.abstractmethod
    def update_after_emulate(self) -> None:
        pass


class BaseNeonTxStrategy(abc.ABC):
    name = 'UNKNOWN STRATEGY'

    def __init__(self, ctx: NeonTxSendCtx):
        self._validation_error_msg: Optional[str] = None
        self._prep_stage_list: List[BaseNeonTxPrepStage] = list()
        self._ctx = ctx
        self._evm_step_cnt = ElfParams().neon_evm_steps

    @property
    def ctx(self) -> NeonTxSendCtx:
        return self._ctx

    @property
    def validation_error_msg(self) -> str:
        assert not self.is_valid()
        return cast(str, self._validation_error_msg)

    def is_valid(self) -> bool:
        return self._validation_error_msg is None

    def validate(self) -> bool:
        self._validation_error_msg = None
        try:
            result = self._validate()
            if result:
                result = self._validate_tx_size()
            assert result == (self._validation_error_msg is None)

            return result
        except Exception as e:
            self._validation_error_msg = str(e)
            return False

    def _validate_notdeploy_tx(self) -> bool:
        if len(self._ctx.neon_tx.toAddress) == 0:
            self._validation_error_msg = 'Deploy transaction'
            return False
        return True

    def _validate_tx_size(self) -> bool:
        tx = self._build_tx()

        # Predefined block_hash is used only to check transaction size, the transaction won't be sent to network
        tx.recent_block_hash = SolBlockHash.from_string('4NCYB3kRT8sCNodPNuCZo8VUh4xqpBQxsxed2wd9xaD4')
        tx.sign(self._ctx.signer)
        tx.serialize()  # <- there will be exception
        return True

    def _validate_tx_has_chainid(self) -> bool:
        if self._ctx.neon_tx.hasChainId():
            return True

        self._validation_error_msg = 'Transaction without chain-id'
        return False

    def prep_before_emulate(self) -> bool:
        assert self.is_valid()

        tx_list_list: List[List[SolTx]] = list()
        for stage in self._prep_stage_list:
            new_tx_list_list = stage.build_prep_tx_list_before_emulate()

            while len(new_tx_list_list) > len(tx_list_list):
                tx_list_list.append(list())
            for tx_list, new_tx_list in zip(tx_list_list, new_tx_list_list):
                tx_list.extend(new_tx_list)

        if len(tx_list_list) == 0:
            return False

        tx_sender = SolTxListSender(self._ctx.config, self._ctx.solana, self._ctx.signer)
        for tx_list in tx_list_list:
            tx_sender.send(tx_list)
        return True

    def update_after_emulate(self) -> None:
        assert self.is_valid()

        for stage in self._prep_stage_list:
            stage.update_after_emulate()

    def _build_cu_tx(self, ix: SolTxIx, name: str = '') -> SolLegacyTx:
        if len(name) == 0:
            name = self.name

        return SolLegacyTx(
            name=name,
            instructions=[
                self._ctx.ix_builder.make_compute_budget_heap_ix(),
                self._ctx.ix_builder.make_compute_budget_cu_ix(),
                ix
            ]
        )

    def _build_cancel_tx(self) -> SolLegacyTx:
        return self._build_cu_tx(name='CancelWithHash', ix=self._ctx.ix_builder.make_cancel_ix())

    @staticmethod
    def _decode_neon_tx_result(tx_send_state_list: List[SolTxSendState], is_canceled: bool) -> NeonTxResultInfo:
        neon_tx_res = NeonTxResultInfo()
        neon_gas_used = 0
        has_good_receipt = False
        is_already_finalized = False
        s = SolTxSendState.Status

        for tx_send_state in tx_send_state_list:
            if tx_send_state.status == s.AlreadyFinalizedError:
                is_already_finalized = True
                continue
            elif tx_send_state.status != s.GoodReceipt:
                continue

            tx_receipt_info = SolTxReceiptInfo.from_tx_receipt(tx_send_state.receipt)
            for sol_neon_ix in tx_receipt_info.iter_sol_neon_ix():
                has_good_receipt = True
                neon_gas_used = max(neon_gas_used, sol_neon_ix.neon_gas_used)

                res = sol_neon_ix.neon_tx_return
                if res is None:
                    continue

                neon_tx_res.set_result(status=res.status, gas_used=res.gas_used)
                LOG.debug(f'Got Neon tx result: {neon_tx_res}')
                return neon_tx_res

        if not neon_tx_res.is_valid():
            if is_already_finalized:
                neon_tx_res.set_result(status=1, gas_used=neon_gas_used)
                LOG.debug(f'Set finalized Neon tx result: {neon_tx_res}')
            elif is_canceled and has_good_receipt:
                neon_tx_res.set_result(status=1, gas_used=neon_gas_used)
                LOG.debug(f'Set canceled Neon tx result: {neon_tx_res}')

        return neon_tx_res

    @abc.abstractmethod
    def _build_tx(self) -> SolLegacyTx:
        pass

    @abc.abstractmethod
    def _validate(self) -> bool:
        pass

    @abc.abstractmethod
    def execute(self) -> NeonTxResultInfo:
        pass
