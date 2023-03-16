import enum
import random
import time
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Set

from ..common_neon.config import Config
from ..common_neon.utils import str_enum
from ..common_neon.errors import CUBudgetExceededError
from ..common_neon.errors import NodeBehindError, NoMoreRetriesError, NonceTooLowError, BlockedAccountsError
from ..common_neon.errors import InvalidIxDataError, RequireResizeIterError, BlockHashNotFound
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.solana_tx import SolTx, SolBlockHash, SolTxReceipt, SolAccount, Commitment
from ..common_neon.solana_tx_error_parser import SolTxErrorParser, SolTxError


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class SolTxSendState:
    class Status(enum.Enum):
        WaitForReceipt = enum.auto()
        NoReceipt = enum.auto()
        GoodReceipt = enum.auto()

        NodeBehindError = enum.auto()
        BadNonceError = enum.auto()
        AltInvalidIndexError = enum.auto()
        AlreadyFinalizedError = enum.auto()
        LogTruncatedError = enum.auto()
        BlockedAccountError = enum.auto()
        CUBudgetExceededError = enum.auto()
        BlockHashNotFoundError = enum.auto()
        AccountAlreadyExistsError = enum.auto()
        InvalidIxDataError = enum.auto()
        RequireResizeIterError = enum.auto()
        UnknownError = enum.auto()

    status: Status
    tx: SolTx
    receipt: SolTxReceipt
    error: Optional[BaseException]

    @property
    def sig(self) -> str:
        if not hasattr(self, '_sig'):
            object.__setattr__(self, '_sig', str(self.tx.signature))
        return object.__getattribute__(self, '_sig')

    @property
    def block_slot(self) -> Optional[int]:
        if not hasattr(self, '_block_slot'):
            object.__setattr__(self, '_block_slot', self.receipt.get('slot', None))
        return object.__getattribute__(self, '_block_slot')


class SolTxListSender:
    _one_block_time = 0.4
    _commitment_set = Commitment.upper_set(Commitment.Confirmed)

    def __init__(self, config: Config, solana: SolInteractor, signer: SolAccount,
                 skip_preflight: Optional[bool] = None):
        self._config = config
        self._solana = solana
        self._signer = signer
        self._skip_preflight = skip_preflight if skip_preflight is not None else config.skip_preflight
        self._retry_idx = 0
        self._block_hash: Optional[SolBlockHash] = None
        self._base_block_slot: Optional[int] = None
        self._bad_block_hash_set: Set[SolBlockHash] = set()
        self._tx_status_dict: Dict[str, SolTxSendState] = dict()
        self._tx_state_list_dict: Dict[SolTxSendState.Status, List[SolTxSendState]] = dict()

    def _clear(self) -> None:
        self._retry_idx = 0
        self._block_hash = None
        self._base_block_slot = None
        self._tx_status_dict.clear()
        self._tx_state_list_dict.clear()

    def send(self, tx_list: List[SolTx]) -> List[SolTxSendState]:
        self._clear()
        retry_on_fail = self._config.retry_on_fail

        while (self._retry_idx < retry_on_fail) and (len(tx_list) > 0):
            self._retry_idx += 1
            self._sign_tx_list(tx_list)
            self._send_tx_list(tx_list)
            LOG.debug(f'retry {self._retry_idx} sending stat: {self._fmt_stat(self._tx_state_list_dict)}')

            tx_list = self._get_tx_list_for_send()
            if len(tx_list) == 0:
                self._wait_for_tx_receipt_list()
                LOG.debug(f'retry {self._retry_idx} waiting stat: {self._fmt_stat(self._tx_state_list_dict)}')
                tx_list = self._get_tx_list_for_send()

        if len(tx_list) > 0:
            raise NoMoreRetriesError()
        return list(self._tx_status_dict.values())

    @staticmethod
    def _fmt_stat(tx_state_list_dict: Dict[SolTxSendState.Status, List[SolTxSendState]]) -> str:
        if not LOG.isEnabledFor(logging.DEBUG):
            return ''

        result = ''
        for tx_status in list(SolTxSendState.Status):
            if tx_status not in tx_state_list_dict:
                continue

            name = str_enum(tx_status)
            cnt = len(tx_state_list_dict[tx_status])

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

        if self._block_hash is None:
            self._block_hash = self._solana.get_recent_block_hash()
            if self._block_hash in self._bad_block_hash_set:
                raise BlockHashNotFound()

        return self._block_hash

    def _sign_tx_list(self, tx_list: List[SolTx]) -> None:
        fuzz_testing = self._config.fuzz_testing
        block_hash = self._get_block_hash()

        for tx in tx_list:
            if tx.is_signed:
                tx_sig = str(tx.signature)
                self._tx_status_dict.pop(tx_sig, None)
                if tx.recent_block_hash in self._bad_block_hash_set:
                    tx.recent_block_hash = None

            if tx.recent_block_hash is None:
                # Fuzz testing of bad blockhash
                if fuzz_testing and (random.randint(0, 3) == 1):
                    tx.recent_block_hash = self._get_fuzz_block_hash()
                # <- Fuzz testing
                else:
                    tx.recent_block_hash = block_hash
                tx.sign(self._signer)

    def _send_tx_list(self, tx_list: List[SolTx]) -> None:
        fuzz_testing = self._config.fuzz_testing
        self._sign_tx_list(tx_list)

        # Fuzz testing of skipping of txs by Solana node
        skipped_tx_list: List[SolTx] = list()
        if fuzz_testing and (len(tx_list) > 1):
            flag_list = [random.randint(0, 5) != 1 for _ in tx_list]
            skipped_tx_list = [tx for tx, flag in zip(tx_list, flag_list) if not flag]
            tx_list = [tx for tx, flag in zip(tx_list, flag_list) if flag]
        # <- Fuzz testing

        LOG.debug(f'send transactions: {self._fmt_tx_name_stat(tx_list)}')
        send_result_list = self._solana.send_tx_list(tx_list, self._skip_preflight)

        no_receipt_status = SolTxSendState.Status.WaitForReceipt
        for tx, send_result in zip(tx_list, send_result_list):
            tx_receipt = send_result.error if send_result.result is None else None
            self._add_tx_state(tx, tx_receipt, no_receipt_status)

        # Fuzz testing of skipping of txs by Solana node
        for tx in skipped_tx_list:
            self._add_tx_state(tx, None, no_receipt_status)
        # <- Fuzz testing

    @staticmethod
    def _fmt_tx_name_stat(tx_list: List[SolTx]) -> str:
        if not LOG.isEnabledFor(logging.DEBUG):
            return ''

        tx_name_dict: Dict[str, int] = dict()
        for tx in tx_list:
            tx_name = tx.name if len(tx.name) > 0 else 'Unknown'
            tx_name_dict[tx_name] = tx_name_dict.get(tx_name, 0) + 1

        return ' + '.join([f'{name}({cnt})' for name, cnt in tx_name_dict.items()])

    def _get_tx_list_for_send(self) -> List[SolTx]:
        s = SolTxSendState.Status
        if s.AlreadyFinalizedError in self._tx_state_list_dict:
            return list()

        good_tx_status_set = {
            s.WaitForReceipt,
            s.GoodReceipt,
            s.LogTruncatedError,
            s.AccountAlreadyExistsError,
        }

        tx_list: List[SolTx] = list()
        for tx_status in list(s):
            if tx_status in good_tx_status_set:
                continue

            tx_state_list = self._tx_state_list_dict.pop(tx_status, None)
            if tx_state_list is None:
                continue

            tx_list.extend(self._convert_state_to_tx_list(tx_state_list))
        return tx_list

    def _wait_for_tx_receipt_list(self) -> None:
        s = SolTxSendState.Status
        tx_state_list = self._tx_state_list_dict.pop(s.WaitForReceipt, list())
        if len(tx_state_list) == 0:
            LOG.debug('No new receipts, because transaction list is empty')
            return

        tx_sig_list = [tx_state.sig for tx_state in tx_state_list]
        self._wait_for_confirmation_of_tx_list(tx_sig_list)

        tx_receipt_list = self._solana.get_tx_receipt_list(tx_sig_list, Commitment.Confirmed)
        for tx_state, tx_receipt in zip(tx_state_list, tx_receipt_list):
            self._add_tx_state(tx_state.tx, tx_receipt, s.NoReceipt)

    def has_good_receipt_list(self) -> bool:
        return SolTxSendState.Status.GoodReceipt in self._tx_state_list_dict

    @staticmethod
    def _get_tx_list_from_state(tx_state_list: List[SolTxSendState]) -> List[SolTx]:
        return [tx_state.tx for tx_state in tx_state_list]

    def _convert_state_to_tx_list(self, tx_state_list: List[SolTxSendState]) -> List[SolTx]:
        tx_state = tx_state_list[0]

        s = SolTxSendState.Status
        if tx_state.status == s.AltInvalidIndexError:
            time.sleep(self._one_block_time)

        good_tx_status_set = {
            s.NoReceipt,
            s.BlockHashNotFoundError,
            s.AltInvalidIndexError,
        }

        if tx_state.status in good_tx_status_set:
            return self._get_tx_list_from_state(tx_state_list)

        # The first few txs failed on blocked accounts, but the subsequent tx successfully locked the accounts.
        if tx_state.status == s.BlockedAccountError:
            if self.has_good_receipt_list() or (s.WaitForReceipt in self._tx_state_list_dict):
                return self._get_tx_list_from_state(tx_state_list)

        error = tx_state.error or SolTxError(tx_state.receipt)
        raise error

    def _wait_for_confirmation_of_tx_list(self, tx_sig_list: List[str]) -> None:
        confirm_timeout = self._config.confirm_timeout_sec
        confirm_check_delay = float(self._config.confirm_check_msec) / 1000
        elapsed_time = 0.0
        commitment_set = self._commitment_set
        base_block_slot = self._base_block_slot

        while elapsed_time < confirm_timeout:
            is_confirmed = self._solana.check_confirmation_of_tx_sig_list(tx_sig_list, commitment_set, base_block_slot)
            if is_confirmed:
                return

            time.sleep(confirm_check_delay)
            elapsed_time += confirm_check_delay

    @dataclass(frozen=True)
    class _DecodeResult:
        tx_status: SolTxSendState.Status
        error: Optional[BaseException] = None

    def _decode_tx_status(self, tx: SolTx, tx_receipt: Optional[SolTxReceipt]) -> _DecodeResult:
        s = SolTxSendState.Status
        tx_error_parser = SolTxErrorParser(tx_receipt)

        slots_behind = tx_error_parser.get_slots_behind()
        if slots_behind is not None:
            return self._DecodeResult(s.NodeBehindError, NodeBehindError(slots_behind))
        elif tx_error_parser.check_if_block_hash_notfound():
            if tx.recent_block_hash not in self._bad_block_hash_set:
                LOG.debug(f'bad block hash: {tx.recent_block_hash}')
                self._bad_block_hash_set.add(tx.recent_block_hash)
            return self._DecodeResult(s.BlockHashNotFoundError)  # no exception: reset blockhash on tx signing
        elif tx_error_parser.check_if_alt_uses_invalid_index():
            return self._DecodeResult(s.AltInvalidIndexError)    # no exception: sleep on 1 block before getting receipt
        elif tx_error_parser.check_if_already_finalized():
            return self._DecodeResult(s.AlreadyFinalizedError)   # no exception: receipt exists - the goal is reached
        elif tx_error_parser.check_if_accounts_blocked():
            return self._DecodeResult(s.BlockedAccountError, BlockedAccountsError())
        elif tx_error_parser.check_if_account_already_exists():
            return self._DecodeResult(s.AccountAlreadyExistsError)  # no exception: account exists - the goal is reached
        elif tx_error_parser.check_if_invalid_ix_data():
            return self._DecodeResult(s.InvalidIxDataError, InvalidIxDataError())
        elif tx_error_parser.check_if_budget_exceeded():
            return self._DecodeResult(s.CUBudgetExceededError, CUBudgetExceededError())
        elif tx_error_parser.check_if_require_resize_iter():
            return self._DecodeResult(s.RequireResizeIterError, RequireResizeIterError())
        elif tx_error_parser.check_if_error():
            LOG.debug(f'unknown error receipt {str(tx.signature)}: {tx_receipt}')
            return self._DecodeResult(s.UnknownError)            # no exception: will be converted to DEFAULT EXCEPTION

        state_tx_cnt, tx_nonce = tx_error_parser.get_nonce_error()
        if state_tx_cnt is not None:
            # sender is unknown - should be replaced on upper stack level
            return self._DecodeResult(s.BadNonceError, NonceTooLowError('?', tx_nonce, state_tx_cnt))
        elif tx_error_parser.check_if_log_truncated():
            return self._DecodeResult(s.LogTruncatedError)       # no exception: by default this is a good receipt

        return self._DecodeResult(s.GoodReceipt)

    def _add_tx_state(self, tx: SolTx, tx_receipt: Optional[SolTxReceipt], no_receipt_status: SolTxSendState.Status):
        res = self._DecodeResult(no_receipt_status) if tx_receipt is None else self._decode_tx_status(tx, tx_receipt)
        tx_send_state = SolTxSendState(status=res.tx_status, tx=tx, receipt=tx_receipt, error=res.error)

        if no_receipt_status == SolTxSendState.Status.NoReceipt:
            log_fn = LOG.warning if tx_receipt is None else LOG.debug
            log_fn(f'tx status {tx_send_state.sig}: {str_enum(tx_send_state.status)}')

        self._tx_status_dict[tx_send_state.sig] = tx_send_state
        self._tx_state_list_dict.setdefault(tx_send_state.status, list()).append(tx_send_state)
