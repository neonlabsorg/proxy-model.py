import enum
import random
import time
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict

from ..common_neon.config import Config
from ..common_neon.errors import CUBudgetExceededError
from ..common_neon.errors import NodeBehindError, NoMoreRetriesError, NonceTooLowError, BlockedAccountsError
from ..common_neon.errors import InvalidIxDataError, RequireResizeIterError
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.solana_tx import SolTx, SolBlockhash, SolTxReceipt, SolAccount, Commitment
from ..common_neon.solana_tx_error_parser import SolTxErrorParser, SolTxError


LOG = logging.getLogger(__name__)


@dataclass
class SolTxSendState:
    class Status(enum.Enum):
        WaitForReceipt = enum.auto()
        NoReceipt = enum.auto()
        GoodReceipt = enum.auto()

        NodeBehindError = enum.auto()
        BadNonceError = enum.auto()
        AltInvalidIndexError = enum.auto()
        AlreadyFinalizedError = enum.auto()
        BlockedAccountError = enum.auto()
        CUBudgetExceededError = enum.auto()
        BlockhashNotFoundError = enum.auto()
        AccountAlreadyExistsError = enum.auto()
        InvalidIxDataError = enum.auto()
        RequireResizeIterError = enum.auto()
        UnknownError = enum.auto()

    status: Status
    tx: SolTx
    receipt: SolTxReceipt

    @property
    def name(self) -> str:
        return self.tx.name

    @property
    def sig(self) -> str:
        return str(self.tx.signature)


class SolTxListSender:
    _one_block_time = 0.4

    def __init__(self, config: Config, solana: SolInteractor, signer: SolAccount,
                 skip_preflight: Optional[bool] = None):
        self._config = config
        self._solana = solana
        self._signer = signer
        self._skip_preflight = skip_preflight if skip_preflight is not None else config.skip_preflight
        self._retry_idx = 0
        self._blockhash: Optional[SolBlockhash] = None
        self._tx_state_dict: Dict[SolTxSendState.Status, List[SolTxSendState]] = dict()
        self._commitment_set = set(Commitment.CommitmentOrder[Commitment.level(config.commit_level):])

    def clear(self) -> None:
        self._retry_idx = 0
        self._blockhash = None
        self._tx_state_dict.clear()

    def send(self, tx_list: List[SolTx]) -> None:
        self.clear()
        while (self._retry_idx < self._config.retry_on_fail) and (len(tx_list) > 0):
            self._retry_idx += 1
            self._send_tx_list(tx_list)
            LOG.debug(f'retry {self._retry_idx} sending stat: {self._fmt_stat()}')

            tx_list = self._get_tx_list_for_send()
            if len(tx_list) == 0:
                self._wait_for_tx_receipt_list()
                LOG.debug(f'retry {self._retry_idx} waiting stat: {self._fmt_stat()}')
                tx_list = self._get_tx_list_for_send()

        if len(tx_list) > 0:
            raise NoMoreRetriesError()

    def _fmt_stat(self) -> str:
        result = ''
        for tx_status in list(SolTxSendState.Status):
            if tx_status not in self._tx_state_dict:
                continue
            name = str(tx_status)
            idx = name.find('.')
            if idx != -1:
                name = name[idx + 1:]
            if len(result) > 0:
                result += ', '
            result += f'{name} {len(self._tx_state_dict[tx_status])}'
        return result

    def _send_tx_list(self, tx_list: List[SolTx]) -> None:
        tx_name_dict: Dict[str, int] = dict()
        for tx in tx_list:
            if LOG.isEnabledFor(logging.DEBUG):
                tx_name = tx.name if len(tx.name) > 0 else 'Unknown'
                tx_name_dict[tx_name] = tx_name_dict.get(tx_name, 0) + 1

            if tx.recent_blockhash is None:
                # Fuzz testing of bad blockhash
                if self._config.fuzz_testing and (random.randint(0, 3) == 1):
                    tx.recent_blockhash = self._get_fuzz_block_hash()
                # <- Fuzz testing
                else:
                    tx.recent_blockhash = self._get_blockhash()
                tx.sign(self._signer)

        # Fuzz testing of skipping of txs by Solana node
        skipped_tx_list: List[SolTx] = list()
        if self._config.fuzz_testing and (len(tx_list) > 1):
            flag_list = [random.randint(0, 5) != 1 for _ in tx_list]
            skipped_tx_list = [tx for tx, flag in zip(tx_list, flag_list) if not flag]
            tx_list = [tx for tx, flag in zip(tx_list, flag_list) if flag]
        # <- Fuzz testing

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug(f'send transactions: {" + ".join([f"{k}({v})" for k, v in tx_name_dict.items()])}')
        send_result_list = self._solana.send_tx_list(tx_list, self._skip_preflight)

        no_receipt_status = SolTxSendState.Status.WaitForReceipt
        for tx, send_result in zip(tx_list, send_result_list):
            tx_receipt = send_result.error if send_result.result is None else None
            self._add_tx_state(tx, tx_receipt, no_receipt_status)

        # Fuzz testing of skipping of txs by Solana node
        for tx in skipped_tx_list:
            self._add_tx_state(tx, None, no_receipt_status)
        # <- Fuzz testing

    def _get_tx_list_for_send(self) -> List[SolTx]:
        s = SolTxSendState.Status
        good_tx_status_set = {
            s.WaitForReceipt,
            s.GoodReceipt,
            s.AlreadyFinalizedError,
            s.AccountAlreadyExistsError,
        }

        tx_list: List[SolTx] = list()
        for tx_status in list(SolTxSendState.Status):
            if tx_status in good_tx_status_set:
                continue
            elif tx_status not in self._tx_state_dict:
                continue

            tx_state_list = self._tx_state_dict.pop(tx_status)
            tx_list.extend(self._convert_state_to_tx_list(tx_status, tx_state_list))
        return tx_list

    def _wait_for_tx_receipt_list(self) -> None:
        tx_state_list = self._tx_state_dict.pop(SolTxSendState.Status.WaitForReceipt, list())
        if len(tx_state_list) == 0:
            LOG.debug('No new receipts, because transaction list is empty')
            return

        tx_sig_list = [tx_state.sig for tx_state in tx_state_list]
        self._wait_for_confirmation_of_tx_list(tx_sig_list)

        tx_receipt_list = self._solana.get_tx_receipt_list(tx_sig_list, self._config.commit_level)
        for tx_state, tx_receipt in zip(tx_state_list, tx_receipt_list):
            self._add_tx_state(tx_state.tx, tx_receipt, SolTxSendState.Status.NoReceipt)

    def _has_good_receipt_list(self) -> bool:
        return (SolTxSendState.Status.GoodReceipt in self._tx_state_dict) or self._has_waiting_tx_list()

    def _has_waiting_tx_list(self) -> bool:
        return SolTxSendState.Status.WaitForReceipt in self._tx_state_dict

    @staticmethod
    def _get_tx_list_from_state(tx_state_list: List[SolTxSendState]) -> List[SolTx]:
        return [tx_state.tx for tx_state in tx_state_list]

    def _convert_state_to_tx_list(self, tx_status: SolTxSendState.Status,
                                  tx_state_list: List[SolTxSendState]) -> List[SolTx]:
        if tx_status == SolTxSendState.Status.AltInvalidIndexError:
            time.sleep(self._one_block_time)

        s = SolTxSendState.Status
        good_tx_status_set = {
            s.NoReceipt,
            s.BlockhashNotFoundError,
            s.AltInvalidIndexError
        }

        if tx_status in good_tx_status_set:
            return self._get_tx_list_from_state(tx_state_list)

        error_tx_status_dict = {
            s.NodeBehindError: NodeBehindError,
            s.BadNonceError: NonceTooLowError,
            s.BlockedAccountError: BlockedAccountsError,
            s.CUBudgetExceededError: CUBudgetExceededError,
            s.InvalidIxDataError: InvalidIxDataError,
            s.RequireResizeIterError: RequireResizeIterError
        }

        e = error_tx_status_dict.get(tx_status, SolTxError)
        raise e(tx_state_list[0].receipt)

    def _wait_for_confirmation_of_tx_list(self, tx_sig_list: List[str]) -> None:
        confirm_timeout = self._config.confirm_timeout_sec
        confirm_check_delay = float(self._config.confirm_check_msec) / 1000
        elapsed_time = 0.0
        not_confirmed_tx_sig = ''

        while elapsed_time < confirm_timeout:
            elapsed_time += confirm_check_delay

            not_confirmed_tx_sig = self._solana.check_tx_sig_list_commitment(tx_sig_list, self._commitment_set)
            if len(not_confirmed_tx_sig) == 0:
                LOG.debug(f'Got confirmed status for transactions: {tx_sig_list}')
                return
            time.sleep(confirm_check_delay)

        LOG.warning(f'No confirmed status for {not_confirmed_tx_sig} from: {tx_sig_list}')

    def _get_fuzz_block_hash(self) -> SolBlockhash:
        block_slot = max(self._solana.get_recent_block_slot() - 525, 10)
        return self._solana.get_blockhash(block_slot)

    def _get_blockhash(self) -> SolBlockhash:
        if self._blockhash is None:
            self._blockhash = self._solana.get_recent_blockhash()
        return self._blockhash

    def _decode_tx_status(self, tx: SolTx, tx_error_parser: SolTxErrorParser) -> SolTxSendState.Status:
        s = SolTxSendState.Status

        slots_behind = tx_error_parser.get_slots_behind()
        if slots_behind is not None:
            LOG.warning(f'Node is behind by {slots_behind} slots')
            return s.NodeBehindError
        elif tx_error_parser.check_if_blockhash_notfound():
            if tx.recent_blockhash == self._blockhash:
                self._blockhash = None
            tx.recent_blockhash = None
            return s.BlockhashNotFoundError
        elif tx_error_parser.check_if_alt_uses_invalid_index():
            return s.AltInvalidIndexError
        elif tx_error_parser.check_if_already_finalized():
            return s.AlreadyFinalizedError
        elif tx_error_parser.check_if_accounts_blocked():
            return s.BlockedAccountError
        elif tx_error_parser.check_if_account_already_exists():
            return s.AccountAlreadyExistsError
        elif tx_error_parser.check_if_invalid_ix_data():
            return s.InvalidIxDataError
        elif tx_error_parser.check_if_budget_exceeded():
            return s.CUBudgetExceededError
        elif tx_error_parser.check_if_require_resize_iter():
            return s.RequireResizeIterError
        elif tx_error_parser.check_if_error():
            LOG.debug(f'unknown_error_receipt {str(tx.signature)}: {tx_error_parser.receipt}')
            return SolTxSendState.Status.UnknownError

        state_tx_cnt, tx_nonce = tx_error_parser.get_nonce_error()
        if state_tx_cnt is not None:
            LOG.debug(f'tx nonce {tx_nonce} != state tx count {state_tx_cnt}')
            return s.BadNonceError

        # store the latest successfully used blockhash
        self._blockhash = tx.recent_blockhash
        return SolTxSendState.Status.GoodReceipt

    def _add_tx_state(self, tx: SolTx, tx_receipt: Optional[SolTxReceipt], no_receipt_status: SolTxSendState.Status):
        tx_status = no_receipt_status
        if tx_receipt is not None:
            tx_status = self._decode_tx_status(tx, SolTxErrorParser(tx_receipt))

        self._tx_state_dict.setdefault(tx_status, list()).append(
            SolTxSendState(
                status=tx_status,
                tx=tx,
                receipt=tx_receipt
            )
        )
