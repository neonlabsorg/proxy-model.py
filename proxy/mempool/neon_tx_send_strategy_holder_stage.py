import copy
import logging

from typing import List

from ..common_neon.evm_config import EVMConfig
from ..common_neon.errors import BadResourceError, HolderContentError, StuckTxError
from ..common_neon.solana_tx import SolTx
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.data import NeonEmulatorResult
from ..common_neon.neon_instruction import EvmIxCodeName, EvmIxCode

from ..neon_core_api.neon_layouts import HolderStatus, HolderAccountInfo

from .neon_tx_send_base_strategy import BaseNeonTxPrepStage


LOG = logging.getLogger(__name__)


class WriteHolderNeonTxPrepStage(BaseNeonTxPrepStage):
    name = EvmIxCodeName().get(EvmIxCode.HolderWrite)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._holder_status = HolderStatus.Empty

    @property
    def holder_status(self) -> HolderStatus:
        return self._holder_status

    def complete_init(self) -> None:
        if not self._ctx.is_stuck_tx():
            self._ctx.mark_resource_use()

    def _validate_holder_account(self) -> None:
        holder_info = self._get_holder_account_info()
        holder_acct = holder_info.holder_account
        self._holder_status = holder_info.status

        if holder_info.status == HolderStatus.Finalized:
            if not self._ctx.has_sol_tx(self.name):
                return
            elif holder_info.neon_tx_sig != self._ctx.neon_tx_info.sig:
                HolderContentError(str(holder_acct))

        elif holder_info.status == HolderStatus.Active:
            if holder_info.neon_tx_sig != self._ctx.neon_tx_info.sig:
                raise StuckTxError(holder_info.neon_tx_sig, holder_info.chain_id, str(holder_acct))

        elif holder_info.status == HolderStatus.Holder:
            if not self._ctx.has_sol_tx(self.name):
                return

    def validate_stuck_tx(self) -> None:
        holder_info = self._get_holder_account_info()
        holder_acct = holder_info.holder_account
        self._holder_status = holder_info.status

        if holder_info.status == HolderStatus.Finalized:
            pass

        elif holder_info.status == HolderStatus.Active:
            if holder_info.neon_tx_sig != self._ctx.neon_tx_info.sig:
                self._holder_status = HolderStatus.Finalized
                LOG.debug(f'NeonTx in {str(holder_acct)} was finished...')
            else:
                self._read_blocked_account_list(holder_info)

        elif holder_info.status == HolderStatus.Holder:
            self._holder_status = HolderStatus.Finalized
            LOG.debug(f'NeonTx in {str(holder_acct)} was finished...')

    def _read_blocked_account_list(self, holder_info: HolderAccountInfo) -> None:
        acct_list = [
            dict(pubkey=str(acct.pubkey), is_writable=acct.is_writable)
            for acct in holder_info.account_list
        ]
        LOG.debug(f'Accounts in holder_list: {[str(acct.pubkey) for acct in holder_info.account_list]}')

        emulator_result = NeonEmulatorResult(dict(
            steps_executed=1,
            predefined_account_order=True,
            solana_accounts=acct_list
        ))
        self._ctx.set_emulator_result(emulator_result)

    def _get_holder_account_info(self) -> HolderAccountInfo:
        holder_account = self._ctx.holder_account

        holder_info = self._ctx.core_api_client.get_holder_account_info(holder_account)
        if holder_info is None:
            raise BadResourceError(f'Bad holder account {str(holder_account)}')
        elif holder_info.status not in {HolderStatus.Finalized, HolderStatus.Active, HolderStatus.Holder}:
            raise BadResourceError(f'Holder account {str(holder_account)} has bad tag: {holder_info.status}')

        self._holder_status = holder_info.status
        return holder_info

    def get_tx_name_list(self) -> List[str]:
        if self._ctx.is_stuck_tx():
            return list()
        return [self.name]

    def build_tx_list(self) -> List[List[SolTx]]:
        if self._ctx.is_stuck_tx() or self._ctx.has_sol_tx(self.name):
            return list()

        builder = self._ctx.ix_builder

        tx_list: List[SolTx] = list()
        holder_msg_offset = 0
        holder_msg = copy.copy(builder.holder_msg)

        holder_msg_size = 900
        while len(holder_msg):
            (holder_msg_part, holder_msg) = (holder_msg[:holder_msg_size], holder_msg[holder_msg_size:])
            ix_list = []
            if self._cu_priority_fee:
                ix_list.append(builder.make_compute_budget_cu_fee_ix(self._cu_priority_fee))
            ix_list.append(builder.make_write_ix(holder_msg_offset, holder_msg_part))
            tx = SolLegacyTx(name=self.name, ix_list=ix_list)
            tx_list.append(tx)
            holder_msg_offset += holder_msg_size

        return [tx_list]

    def update_holder_tag(self) -> None:
        if self._ctx.is_stuck_tx():
            self.validate_stuck_tx()
        else:
            self._validate_holder_account()

    def update_after_emulate(self) -> None:
        self.update_holder_tag()
