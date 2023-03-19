import enum
import logging
import random
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Set

from ..common_neon.config import Config
from ..common_neon.errors import RescheduleError, WrongStrategyError
from ..common_neon.errors import BlockHashNotFound, NonceTooLowError
from ..common_neon.errors import CUBudgetExceededError, InvalidIxDataError, RequireResizeIterError
from ..common_neon.errors import CommitLevelError, NodeBehindError, NoMoreRetriesError, BlockedAccountsError
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.solana_tx import SolTx, SolBlockHash, SolTxReceipt, SolAccount, Commitment
from ..common_neon.solana_tx_error_parser import SolTxErrorParser, SolTxError
from ..common_neon.utils import str_enum

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class SolTxSendState:
    class Status(enum.Enum):
        # Good receipts
        WaitForReceipt = enum.auto()
        GoodReceipt = enum.auto()
        LogTruncatedError = enum.auto()

        # Skipped errors
        AccountAlreadyExistsError = enum.auto()
        AlreadyFinalizedError = enum.auto()

        # Resubmitted errors
        NoReceiptError = enum.auto()
        BlockHashNotFoundError = enum.auto()
        AltInvalidIndexError = enum.auto()

        # Rescheduling errors
        NodeBehindError = enum.auto()
        BlockedAccountError = enum.auto()

        # Wrong strategy error
        CUBudgetExceededError = enum.auto()
        InvalidIxDataError = enum.auto()
        RequireResizeIterError = enum.auto()

        # Fail errors
        BadNonceError = enum.auto()
        UnknownError = enum.auto()

    status: Status
    tx: SolTx
    valid_block_height: int
    receipt: Optional[SolTxReceipt]
    error: Optional[BaseException]

    @property
    def sig(self) -> str:
        return str(self.tx.signature)

    @property
    def block_slot(self) -> Optional[int]:
        if self.receipt is None:
            return None
        return self.receipt.get('slot')

    @property
    def name(self) -> str:
        return self.tx.name


class SolTxListSender:
    _one_block_time = 0.4
    _commitment_set = Commitment.upper_set(Commitment.Confirmed)
    _big_block_height = 2 ** 64 - 1
    _big_block_slot = 2 ** 64 - 1

    def __init__(self, config: Config, solana: SolInteractor, signer: SolAccount):
        self._config = config
        self._solana = solana
        self._signer = signer
        self._block_hash: Optional[SolBlockHash] = None
        self._valid_block_height = self._big_block_height
        self._block_hash_dict: Dict[SolBlockHash, int] = dict()
        self._bad_block_hash_set: Set[SolBlockHash] = set()
        self._tx_list: List[SolTx] = list()
        self._tx_state_dict: Dict[str, SolTxSendState] = dict()
        self._tx_state_list_dict: Dict[SolTxSendState.Status, List[SolTxSendState]] = dict()

    def send(self, tx_list: List[SolTx]) -> bool:
        self._clear()
        if len(tx_list) == 0:
            return False

        self._tx_list = tx_list
        return self._send()

    def recheck(self, tx_state_list: List[SolTxSendState]) -> bool:
        self._clear()
        if len(tx_state_list) == 0:
            return False

        # We should check all (failed too) txs again, because the state can be changed
        tx_sig_list = [tx_state.sig for tx_state in tx_state_list]
        self._get_tx_receipt_list(tx_sig_list, tx_state_list)

        self._get_tx_list_for_send()
        return self._send()

    @property
    def tx_state_list(self) -> List[SolTxSendState]:
        return list(self._tx_state_dict.values())

    def has_receipt(self) -> bool:
        return len(self._tx_state_dict) > 0

    def _clear(self) -> None:
        self._block_hash = None
        self._tx_list.clear()
        self._tx_state_dict.clear()
        self._tx_state_list_dict.clear()

    def _send(self) -> bool:
        try:
            self._send_impl()
            self._validate_commit_level()
            return True  # always True, because we send txs
        except (WrongStrategyError, RescheduleError):
            raise
        except (BaseException,):
            self._validate_commit_level()
            raise

    def _send_impl(self) -> None:
        retry_on_fail = self._config.retry_on_fail

        for retry_idx in range(retry_on_fail):
            self._sign_tx_list()
            self._send_tx_list()
            LOG.debug(f'retry {retry_idx} sending stat: {self._fmt_stat()}')

            # get txs with preflight check errors for resubmitting
            self._get_tx_list_for_send()
            if len(self._tx_list) != 0:
                continue

            # get receipts from network
            self._wait_for_tx_receipt_list()
            LOG.debug(f'retry {retry_idx} waiting stat: {self._fmt_stat()}')

            self._get_tx_list_for_send()
            if len(self._tx_list) == 0:
                break

        if len(self._tx_list) > 0:
            raise NoMoreRetriesError()

    def _validate_commit_level(self) -> None:
        commit_level = self._config.commit_level
        if commit_level == Commitment.Confirmed:
            return

        # find minimal block slot
        min_block_slot = self._big_block_slot
        for tx_state in self._tx_state_dict.values():
            tx_block_slot = tx_state.block_slot
            if tx_block_slot is not None:
                min_block_slot = min(min_block_slot, tx_block_slot)

        min_block_status = self._solana.get_block_status(min_block_slot)
        if Commitment.level(min_block_status.commitment) < Commitment.level(commit_level):
            raise CommitLevelError(commit_level, min_block_status.commitment)

    def _fmt_stat(self) -> str:
        if not LOG.isEnabledFor(logging.DEBUG):
            return ''

        result = ''
        for tx_status in list(SolTxSendState.Status):
            if tx_status not in self._tx_state_list_dict:
                continue

            name = str_enum(tx_status)
            cnt = len(self._tx_state_list_dict[tx_status])

            if len(result) > 0:
                result += ', '
            result += f'{name} {cnt}'
        return result

    def _get_fuzz_block_hash(self) -> SolBlockHash:
        block_slot = max(self._solana.get_recent_block_slot() - random.randint(525, 1025), 2)
        block_hash = self._solana.get_block_hash(block_slot)
        LOG.debug(f'fuzzing block hash: {block_hash}')
        return block_hash

    def _get_block_hash(self) -> SolBlockHash:
        if self._block_hash in self._bad_block_hash_set:
            self._block_hash = None

        if self._block_hash is not None:
            return self._block_hash

        resp = self._solana.get_recent_block_hash()
        if resp.block_hash in self._bad_block_hash_set:
            raise BlockHashNotFound()

        self._block_hash = resp.block_hash
        self._block_hash_dict[resp.block_hash] = resp.last_valid_block_height

        return self._block_hash

    def _sign_tx_list(self) -> None:
        fuzz_testing = self._config.fuzz_testing
        block_hash = self._get_block_hash()

        for tx in self._tx_list:
            if tx.is_signed:
                tx_sig = str(tx.signature)
                self._tx_state_dict.pop(tx_sig, None)
                if tx.recent_block_hash in self._bad_block_hash_set:
                    tx.recent_block_hash = None

            if tx.recent_block_hash is not None:
                continue

            # Fuzz testing of bad blockhash
            if fuzz_testing and (random.randint(0, 3) == 1):
                tx.recent_block_hash = self._get_fuzz_block_hash()
            # <- Fuzz testing
            else:
                tx.recent_block_hash = block_hash
            tx.sign(self._signer)

    def _send_tx_list(self) -> None:
        fuzz_testing = self._config.fuzz_testing

        # Fuzz testing of skipping of txs by Solana node
        skipped_tx_list: List[SolTx] = list()
        if fuzz_testing and (len(self._tx_list) > 1):
            flag_list = [random.randint(0, 5) != 1 for _ in self._tx_list]
            skipped_tx_list = [tx for tx, flag in zip(self._tx_list, flag_list) if not flag]
            self._tx_list = [tx for tx, flag in zip(self._tx_list, flag_list) if flag]
        # <- Fuzz testing

        LOG.debug(f'send transactions: {self._fmt_tx_name_stat()}')
        send_result_list = self._solana.send_tx_list(self._tx_list, skip_preflight=False)

        no_receipt_status = SolTxSendState.Status.WaitForReceipt
        for tx, send_result in zip(self._tx_list, send_result_list):
            tx_receipt = send_result.error if send_result.result is None else None
            self._add_tx_state(tx, tx_receipt, no_receipt_status)

        # Fuzz testing of skipping of txs by Solana node
        for tx in skipped_tx_list:
            self._add_tx_state(tx, None, no_receipt_status)
        # <- Fuzz testing

    def _fmt_tx_name_stat(self) -> str:
        if not LOG.isEnabledFor(logging.DEBUG):
            return ''

        tx_name_dict: Dict[str, int] = dict()
        for tx in self._tx_list:
            tx_name = tx.name if len(tx.name) > 0 else 'Unknown'
            tx_name_dict[tx_name] = tx_name_dict.get(tx_name, 0) + 1

        return ' + '.join([f'{name}({cnt})' for name, cnt in tx_name_dict.items()])

    def _get_tx_list_for_send(self) -> None:
        self._tx_list.clear()
        status = SolTxSendState.Status

        # the Neon tx is finalized in another Solana tx
        if status.AlreadyFinalizedError in self._tx_state_list_dict:
            return

        remove_tx_status_set: Set[SolTxSendState.Status] = set()
        for tx_status in list(status):
            if not self._check_tx_status_for_send(tx_status):
                continue

            remove_tx_status_set.add(tx_status)
            tx_state_list = self._tx_state_list_dict.get(tx_status)
            for tx_state in tx_state_list:
                self._tx_list.append(tx_state.tx)

        for tx_status in remove_tx_status_set:
            self._tx_state_list_dict.pop(tx_status)

    def _check_tx_status_for_send(self, tx_status: SolTxSendState.Status) -> bool:
        status = SolTxSendState.Status

        completed_tx_status_set = {
            status.WaitForReceipt,
            status.GoodReceipt,
            status.LogTruncatedError,
            status.AccountAlreadyExistsError,
        }
        if tx_status in completed_tx_status_set:
            return False

        tx_state_list = self._tx_state_list_dict.get(tx_status, None)
        if (tx_state_list is None) or (len(tx_state_list) == 0):
            return False

        if tx_status == status.AltInvalidIndexError:
            time.sleep(self._one_block_time)

        resubmitted_tx_status_set = {
            status.NoReceiptError,
            status.BlockHashNotFoundError,
            status.AltInvalidIndexError,
        }
        if tx_status in resubmitted_tx_status_set:
            return True

        # The first few txs failed on blocked accounts, but the subsequent tx successfully locked the accounts.
        if tx_status == status.BlockedAccountError:
            for completed_status in completed_tx_status_set:
                if completed_status in self._tx_state_list_dict:
                    return True

        tx_state = tx_state_list[0]
        error = tx_state.error or SolTxError(tx_state.receipt)
        raise error

    def _wait_for_tx_receipt_list(self) -> None:
        tx_state_list = self._tx_state_list_dict.pop(SolTxSendState.Status.WaitForReceipt, None)
        if tx_state_list is None:
            LOG.debug('No new receipts, because transaction list is empty')
            return

        tx_sig_list: List[str] = list()
        valid_block_height = self._big_block_height
        for tx_state in tx_state_list:
            tx_sig_list.append(tx_state.sig)
            valid_block_height = min(valid_block_height, tx_state.valid_block_height)

        self._wait_for_confirmation_of_tx_list(tx_sig_list, valid_block_height)
        self._get_tx_receipt_list(tx_sig_list, tx_state_list)

    def _get_tx_receipt_list(self, tx_sig_list: Optional[List[str]], tx_state_list: List[SolTxSendState]) -> None:
        tx_receipt_list = self._solana.get_tx_receipt_list(tx_sig_list, Commitment.Confirmed)
        for tx_state, tx_receipt in zip(tx_state_list, tx_receipt_list):
            self._add_tx_state(tx_state.tx, tx_receipt, SolTxSendState.Status.NoReceiptError)

    def _wait_for_confirmation_of_tx_list(self, tx_sig_list: List[str], valid_block_height: int) -> None:
        confirm_timeout = self._config.confirm_timeout_sec
        confirm_check_delay = float(self._config.confirm_check_msec) / 1000
        elapsed_time = 0.0
        commitment_set = self._commitment_set

        while elapsed_time < confirm_timeout:
            is_confirmed = self._solana.check_confirm_of_tx_sig_list(tx_sig_list, commitment_set, valid_block_height)
            if is_confirmed:
                return

            time.sleep(confirm_check_delay)
            elapsed_time += confirm_check_delay

    @dataclass(frozen=True)
    class _DecodeResult:
        tx_status: SolTxSendState.Status
        error: Optional[BaseException]

    def _decode_tx_status(self, tx: SolTx, tx_receipt: Optional[SolTxReceipt]) -> _DecodeResult:
        status = SolTxSendState.Status
        tx_error_parser = SolTxErrorParser(tx_receipt)

        slots_behind = tx_error_parser.get_slots_behind()
        if slots_behind is not None:
            return self._DecodeResult(status.NodeBehindError, NodeBehindError(slots_behind))
        elif tx_error_parser.check_if_block_hash_notfound():
            if tx.recent_block_hash not in self._bad_block_hash_set:
                LOG.debug(f'bad block hash: {tx.recent_block_hash}')
                self._bad_block_hash_set.add(tx.recent_block_hash)
            # no exception: reset blockhash on tx signing
            return self._DecodeResult(status.BlockHashNotFoundError, None)
        elif tx_error_parser.check_if_alt_uses_invalid_index():
            # no exception: sleep on 1 block before getting receipt
            return self._DecodeResult(status.AltInvalidIndexError, None)
        elif tx_error_parser.check_if_already_finalized():
            # no exception: receipt exists - the goal is reached
            return self._DecodeResult(status.AlreadyFinalizedError, None)
        elif tx_error_parser.check_if_accounts_blocked():
            return self._DecodeResult(status.BlockedAccountError, BlockedAccountsError())
        elif tx_error_parser.check_if_account_already_exists():
            # no exception: account exists - the goal is reached
            return self._DecodeResult(status.AccountAlreadyExistsError, None)
        elif tx_error_parser.check_if_invalid_ix_data():
            return self._DecodeResult(status.InvalidIxDataError, InvalidIxDataError())
        elif tx_error_parser.check_if_budget_exceeded():
            return self._DecodeResult(status.CUBudgetExceededError, CUBudgetExceededError())
        elif tx_error_parser.check_if_require_resize_iter():
            return self._DecodeResult(status.RequireResizeIterError, RequireResizeIterError())
        elif tx_error_parser.check_if_error():
            LOG.debug(f'unknown error receipt {str(tx.signature)}: {tx_receipt}')
            # no exception: will be converted to DEFAULT EXCEPTION
            return self._DecodeResult(status.UnknownError, None)

        state_tx_cnt, tx_nonce = tx_error_parser.get_nonce_error()
        if state_tx_cnt is not None:
            # sender is unknown - should be replaced on upper stack level
            return self._DecodeResult(status.BadNonceError, NonceTooLowError('?', tx_nonce, state_tx_cnt))
        elif tx_error_parser.check_if_log_truncated():
            # no exception: by default this is a good receipt
            return self._DecodeResult(status.LogTruncatedError, None)

        return self._DecodeResult(status.GoodReceipt, None)

    def _add_tx_state(self, tx: SolTx, tx_receipt: Optional[SolTxReceipt], no_receipt_status: SolTxSendState.Status):
        if tx_receipt is None:
            res = self._DecodeResult(no_receipt_status, None)
        else:
            res = self._decode_tx_status(tx, tx_receipt)
        valid_block_height = self._block_hash_dict.get(tx.recent_block_hash, self._big_block_height)

        tx_send_state = SolTxSendState(
            status=res.tx_status,
            tx=tx,
            receipt=tx_receipt,
            error=res.error,
            valid_block_height=valid_block_height
        )

        status = SolTxSendState.Status
        if tx_send_state.status not in {status.WaitForReceipt, status.UnknownError}:
            log_fn = LOG.warning if tx_receipt is None else LOG.debug
            log_fn(f'tx status {tx_send_state.sig}: {str_enum(tx_send_state.status)}')

        self._tx_state_dict[tx_send_state.sig] = tx_send_state
        self._tx_state_list_dict.setdefault(tx_send_state.status, list()).append(tx_send_state)
