from __future__ import annotations

import logging
import enum
import time

from typing import List, Dict, Set, Optional, Tuple, Union, cast

from ..common_neon.utils.neon_tx_info import NeonTxInfo
from ..common_neon.utils.json_logger import logging_context
from ..common_neon.address import NeonAddress

from .mempool_api import (
    MPTxRequest,
    MPTxSendResult,
    MPTxSendResultCode,
    MPSenderTxCntData,
    MPTxRequestList,
    MPTxPoolContentResult,
)
from .sorted_queue import SortedQueue


LOG = logging.getLogger(__name__)


class MPTxRequestDict:
    _top_index = -1

    def __init__(self) -> None:
        self._tx_hash_dict: Dict[str, MPTxRequest] = {}
        self._tx_sender_nonce_dict: Dict[str, MPTxRequest] = {}
        self._tx_gas_price_queue = SortedQueue[MPTxRequest, int, str](
            lt_key_func=lambda a: -a.gas_price, eq_key_func=lambda a: a.sig
        )
        self._tx_gapped_gas_price_queue = SortedQueue[MPTxRequest, int, str](
            lt_key_func=lambda a: -a.gas_price, eq_key_func=lambda a: a.sig
        )

    def __len__(self) -> int:
        return len(self._tx_hash_dict)

    @property
    def len_tx_gas_price_queue(self) -> int:
        return len(self._tx_gas_price_queue)

    @staticmethod
    def _sender_nonce(tx: Union[MPTxRequest, Tuple[str, int]]) -> str:
        if isinstance(tx, MPTxRequest):
            sender_addr, tx_nonce = tx.sender_address, tx.nonce
        else:
            sender_addr, tx_nonce = tx
        return f"{sender_addr}:{tx_nonce}"

    def add_tx(self, tx: MPTxRequest, is_gapped_tx: bool) -> None:
        sender_nonce = self._sender_nonce(tx)
        assert tx.sig not in self._tx_hash_dict, f"Tx {tx.sig} is already in dictionary"
        assert sender_nonce not in self._tx_sender_nonce_dict, f"Tx {sender_nonce} is already in dictionary"
        assert tx not in self._tx_gas_price_queue, f"Tx {tx.sig} is already in gas price queue"
        assert tx not in self._tx_gapped_gas_price_queue, f"Tx {tx.sig} is already in gapped gas price queue"

        self._tx_hash_dict[tx.sig] = tx
        self._tx_sender_nonce_dict[sender_nonce] = tx

        if is_gapped_tx:
            self._tx_gapped_gas_price_queue.add(tx)
        else:
            self._tx_gas_price_queue.add(tx)
            self.queue_tx(tx.sender_address, tx.nonce + 1)

        assert (
            len(self._tx_hash_dict)
            == len(self._tx_sender_nonce_dict)
            >= (len(self._tx_gas_price_queue) + len(self._tx_gapped_gas_price_queue))
        )

    def pop_tx(self, tx: MPTxRequest) -> MPTxRequest:
        assert tx.sig in self._tx_hash_dict, f"Tx {tx.sig} is absent in dictionary"

        sender_nonce = self._sender_nonce(tx)
        assert sender_nonce in self._tx_sender_nonce_dict, f"Tx {sender_nonce} is absent in dictionary"

        # tx may be removed from the gas price queue on processing
        if (pos := self._tx_gapped_gas_price_queue.find(tx)) is not None:
            self._tx_gapped_gas_price_queue.pop(pos)
        else:
            self._tx_gas_price_queue.pop(tx)
            self.dequeue_tx(tx.sender_address, tx.nonce + 1)

        self._tx_sender_nonce_dict.pop(sender_nonce)
        return self._tx_hash_dict.pop(tx.sig)

    def done_tx(self, tx: MPTxRequest, is_suspended: bool) -> MPTxRequest:
        """Tx was in the processing,"""
        assert tx.sig in self._tx_hash_dict, f"Tx {tx.sig} is absent in dictionary"

        sender_nonce = self._sender_nonce(tx)
        assert sender_nonce in self._tx_sender_nonce_dict, f"Tx {sender_nonce} is absent in dictionary"
        assert tx not in self._tx_gas_price_queue
        assert tx not in self._tx_gapped_gas_price_queue

        if is_suspended:
            self.dequeue_tx(tx.sender_address, tx.nonce + 1)

        self._tx_sender_nonce_dict.pop(sender_nonce)
        return self._tx_hash_dict.pop(tx.sig)

    def _move_between_gas_price_queues(
        self,
        src: SortedQueue[MPTxRequest, int, str],
        dst: SortedQueue[MPTxRequest, int, str],
        sender_address: str,
        nonce: int,
    ) -> None:
        while True:
            sender_nonce = self._sender_nonce((sender_address, nonce))
            if (tx := self._tx_sender_nonce_dict.get(sender_nonce, None)) is None:
                break
            dst.add(src.pop(tx))
            nonce += 1

    def get_tx_by_hash(self, neon_sig: str) -> Optional[MPTxRequest]:
        return self._tx_hash_dict.get(neon_sig, None)

    def get_tx_by_sender_nonce(self, sender_addr: str, tx_nonce: int) -> Optional[MPTxRequest]:
        return self._tx_sender_nonce_dict.get(self._sender_nonce((sender_addr, tx_nonce)), None)

    def acquire_tx(self, tx: MPTxRequest) -> None:
        self._tx_gas_price_queue.pop(tx)

    def cancel_process_tx(self, tx: MPTxRequest, is_suspended: bool) -> None:
        if is_suspended:
            self._tx_gapped_gas_price_queue.add(tx)
            self.dequeue_tx(tx.sender_address, tx.nonce + 1)
        else:
            self._tx_gas_price_queue.add(tx)

    def queue_tx(self, sender_address: str, start_nonce: int) -> None:
        self._move_between_gas_price_queues(
            self._tx_gapped_gas_price_queue, self._tx_gas_price_queue, sender_address, start_nonce
        )

    def dequeue_tx(self, sender_address: str, start_nonce: int) -> None:
        self._move_between_gas_price_queues(
            self._tx_gas_price_queue, self._tx_gapped_gas_price_queue, sender_address, start_nonce
        )

    def peek_gapped_lower_tx(self) -> Optional[MPTxRequest]:
        return self._tx_gapped_gas_price_queue[self._top_index] if len(self._tx_gapped_gas_price_queue) > 0 else None

    def peek_pending_lower_tx(self) -> Optional[MPTxRequest]:
        return self._tx_gas_price_queue[self._top_index] if len(self._tx_gas_price_queue) > 0 else None

    def peek_lower_tx(self) -> Optional[MPTxRequest]:
        return self.peek_gapped_lower_tx() or self.peek_pending_lower_tx()


class MPSenderTxPool:
    _top_index = -1
    _bottom_index = 0

    class State(enum.IntEnum):
        Empty = 1
        Queued = 2
        Processing = 3
        Suspended = 4

    def __init__(self, sender_address: str) -> None:
        self._state = self.State.Empty
        self._sender_address = sender_address
        self._gas_price = 0
        self._heartbeat = int(time.time())
        self._state_tx_cnt = 0
        self._processing_tx: Optional[MPTxRequest] = None
        self._tx_nonce_queue = SortedQueue[MPTxRequest, int, str](
            lt_key_func=lambda a: -a.nonce, eq_key_func=lambda a: a.sig
        )

    @property
    def sender_address(self) -> str:
        return self._sender_address

    @property
    def gas_price(self) -> int:
        return self._gas_price

    @property
    def state(self) -> MPSenderTxPool.State:
        return self._state

    def sync_state(self) -> MPSenderTxPool.State:
        self._state = self._actual_state
        self._gas_price = self.top_tx.gas_price if self._state != self.State.Empty else 0
        return self._state

    def has_valid_state(self) -> bool:
        new_state = self._actual_state
        if new_state != self._state:
            return False
        elif new_state == self.State.Queued:
            return self.top_tx.gas_price == self._gas_price
        return True

    @property
    def _actual_state(self) -> MPSenderTxPool.State:
        if self.is_empty():
            return self.State.Empty
        elif self.is_processing():
            return self.State.Processing
        elif self._state_tx_cnt != self.top_tx.nonce:
            return self.State.Suspended
        return self.State.Queued

    def is_empty(self) -> bool:
        return self.len_tx_nonce_queue == 0

    def is_processing(self) -> bool:
        return self._processing_tx is not None

    @property
    def len_tx_nonce_queue(self) -> int:
        return len(self._tx_nonce_queue)

    def add_tx(self, tx: MPTxRequest) -> None:
        assert self._state_tx_cnt <= tx.nonce, f"Tx {tx.sig} has nonce {tx.nonce} less than {self._state_tx_cnt}"
        self._tx_nonce_queue.add(tx)
        self._heartbeat = int(time.time())

    @property
    def top_tx(self) -> Optional[MPTxRequest]:
        return self._tx_nonce_queue[self._top_index]

    def acquire_tx(self, tx: MPTxRequest) -> MPTxRequest:
        assert not self.is_processing()
        assert tx.sig == self.top_tx.sig

        self._processing_tx = self.top_tx
        self.sync_state()
        return self._processing_tx

    @property
    def pending_nonce(self) -> Optional[int]:
        if self.state in {self.State.Suspended, self.State.Empty}:
            LOG.debug(f"state = {self.state}")
            return None

        pending_nonce = self._state_tx_cnt
        LOG.debug(f"state_tx_cnt = {self._state_tx_cnt}, pending_tx_cnt = {len(self._tx_nonce_queue)}")
        for tx in reversed(self._tx_nonce_queue):
            if tx.nonce != pending_nonce:
                LOG.debug(f"tx.nonce ({tx.nonce}) != pending_nonce {pending_nonce}, state_tx_cnt {self._state_tx_cnt}")
                break
            pending_nonce += 1
        return pending_nonce

    @property
    def last_nonce(self) -> Optional[int]:
        return self._tx_nonce_queue[self._bottom_index].nonce if not self.is_empty() else None

    @property
    def state_tx_cnt(self) -> int:
        if self.is_processing():
            assert self._state_tx_cnt == self._processing_tx.nonce
            return self._processing_tx.nonce + 1
        return self._state_tx_cnt

    def set_state_tx_cnt(self, value: int) -> None:
        self._state_tx_cnt = value

    @property
    def heartbeat(self) -> int:
        return self._heartbeat

    def _validate_processing_tx(self, tx: MPTxRequest) -> None:
        assert not self.is_empty(), f"no transactions in {self.sender_address} pool"
        assert self.is_processing(), f"{self.sender_address} pool does not process tx {tx.sig}"

        t_tx, p_tx = self.top_tx, self._processing_tx
        assert tx.sig == p_tx.sig, f"tx {tx.sig} is not equal to processing tx {p_tx.sig}"
        assert t_tx is p_tx, f"top tx {t_tx.sig} is not equal to processing tx {p_tx.sig}"

    def done_tx(self, tx: MPTxRequest) -> None:
        self._validate_processing_tx(tx)

        self._tx_nonce_queue.pop(self._top_index)
        self._processing_tx = None
        LOG.debug(f"Done tx {tx.sig}. There are {self.len_tx_nonce_queue} txs left in {self.sender_address} pool")

    def drop_tx(self, tx: MPTxRequest) -> None:
        assert not self.is_processing() or tx.sig != self._processing_tx.sig, f"cannot drop processing tx {tx.sig}"

        self._tx_nonce_queue.pop(tx)
        LOG.debug(f"Drop tx {tx.sig}. There are {self.len_tx_nonce_queue} txs left in {self.sender_address} pool")

    def cancel_process_tx(self, tx: MPTxRequest) -> None:
        self._validate_processing_tx(tx)

        self._processing_tx.neon_tx_exec_cfg = tx.neon_tx_exec_cfg
        self._processing_tx = None

    def take_out_tx_list(self) -> MPTxRequestList:
        is_processing = self.is_processing()
        LOG.debug(
            f"Take out txs from sender pool: {self.sender_address}, count: {self.len_tx_nonce_queue}, "
            f"processing: {is_processing}"
        )
        _from = 1 if is_processing else 0
        taken_out_tx_list = self._tx_nonce_queue.extract_list_from(_from)
        return taken_out_tx_list

    @property
    def pending_stop_pos(self) -> int:
        if self.state in {self.State.Suspended, self.State.Empty}:
            return 0

        pending_pos, pending_nonce = 0, self._state_tx_cnt
        for tx in reversed(self._tx_nonce_queue):
            if tx.nonce != pending_nonce:
                break
            pending_nonce += 1
            pending_pos += 1
        return pending_pos

    def tx_list(self) -> MPTxRequestList:
        return list(reversed(self._tx_nonce_queue))


class MPTxSchedule:
    _top_index = -1

    def __init__(self, capacity: int, capacity_high_watermark: float, chain_id: int) -> None:
        self._capacity = capacity
        self._capacity_high_watermark = 0
        self._tx_dict = MPTxRequestDict()
        self._chain_id = chain_id

        self._sender_pool_dict: Dict[str, MPSenderTxPool] = dict()
        self._sender_pool_heartbeat_queue = SortedQueue[MPSenderTxPool, int, str](
            lt_key_func=lambda a: -a.heartbeat, eq_key_func=lambda a: a.sender_address
        )
        self._sender_pool_queue = SortedQueue[MPSenderTxPool, int, str](
            lt_key_func=lambda a: a.gas_price, eq_key_func=lambda a: a.sender_address
        )
        self._suspended_sender_set: Set[NeonAddress] = set()
        self.set_capacity_high_watermark(capacity_high_watermark)

    def set_capacity_high_watermark(self, value: float) -> None:
        """Sets the mempool capacity high watermark as a multiplier of the capacity"""
        self._capacity_high_watermark = int(self._capacity * value)

    @property
    def min_gas_price(self) -> int:
        if self.tx_cnt < self._capacity_high_watermark:
            return 0

        lower_tx = self._tx_dict.peek_pending_lower_tx()
        if not lower_tx:
            return 0
        return int(lower_tx.gas_price * 1.3)  # increase gas-price in 30%

    @property
    def chain_id(self) -> int:
        return self._chain_id

    def _add_tx_to_sender_pool(self, sender_pool: MPSenderTxPool, tx: MPTxRequest, is_gapped_tx: bool) -> None:
        LOG.debug(f"Add tx {tx.sig} to mempool with {self.tx_cnt} txs")

        is_new_pool = sender_pool.state == sender_pool.State.Empty
        if not is_new_pool:
            self._sender_pool_heartbeat_queue.pop(sender_pool)

        sender_pool.add_tx(tx)
        self._tx_dict.add_tx(tx, is_gapped_tx)

        # the first tx in the sender pool
        if is_new_pool:
            self._sender_pool_dict[sender_pool.sender_address] = sender_pool

        self._sender_pool_heartbeat_queue.add(sender_pool)

    def _drop_tx_from_sender_pool(self, sender_pool: MPSenderTxPool, tx: MPTxRequest) -> None:
        LOG.debug(f"Drop tx {tx.sig} from pool {sender_pool.sender_address}")
        sender_pool.drop_tx(tx)
        self._tx_dict.pop_tx(tx)

    def drop_expired_sender_pools(self, eviction_timeout_sec: int) -> None:
        threshold = int(time.time()) - eviction_timeout_sec
        LOG.debug(f"Try to drop sender pools with heartbeat below {threshold}")

        while not self._sender_pool_heartbeat_queue.is_empty():
            sender_pool = self._sender_pool_heartbeat_queue[self._top_index]

            if threshold < sender_pool.heartbeat or sender_pool.is_processing():
                break

            LOG.debug(
                "Dropping sender pool {} with heartbeat {}".format(sender_pool.sender_address, sender_pool.heartbeat)
            )

            while not sender_pool.is_empty():
                tx = sender_pool.top_tx
                self._drop_tx_from_sender_pool(sender_pool, tx)

            self._sync_sender_state(sender_pool)

    def _find_sender_pool(self, sender_address: str) -> Optional[MPSenderTxPool]:
        return self._sender_pool_dict.get(sender_address, None)

    def _get_or_create_sender_pool(self, sender_address: str) -> MPSenderTxPool:
        sender_pool = self._find_sender_pool(sender_address)
        if sender_pool is None:
            sender_pool = MPSenderTxPool(sender_address)
        return sender_pool

    def _get_sender_pool(self, sender_address: str) -> MPSenderTxPool:
        sender_pool = self._find_sender_pool(sender_address)
        assert sender_pool is not None, f"Failed to get sender tx pool by sender address {sender_address}"
        return cast(MPSenderTxPool, sender_pool)

    def _schedule_sender_pool(self, sender_pool: MPSenderTxPool, state_tx_cnt: int) -> None:
        self._set_sender_tx_cnt(sender_pool, state_tx_cnt)
        self._sync_sender_state(sender_pool)

    def _set_sender_tx_cnt(self, sender_pool: MPSenderTxPool, state_tx_cnt: int) -> None:
        if sender_pool.state_tx_cnt == state_tx_cnt:
            return
        elif sender_pool.is_processing():
            return

        while not sender_pool.is_empty():
            top_tx = sender_pool.top_tx
            if top_tx.nonce >= state_tx_cnt:
                break

            self._drop_tx_from_sender_pool(sender_pool, top_tx)

        sender_pool.set_state_tx_cnt(state_tx_cnt)

    def _sync_sender_state(self, sender_pool: MPSenderTxPool) -> None:
        if sender_pool.has_valid_state():
            return

        old_state = sender_pool.state
        if old_state == sender_pool.State.Suspended:
            self._suspended_sender_set.remove(NeonAddress.from_raw(sender_pool.sender_address, self._chain_id))
        elif old_state == sender_pool.State.Queued:
            self._sender_pool_queue.pop(sender_pool)

        new_state = sender_pool.sync_state()
        if new_state == sender_pool.State.Empty:
            self._sender_pool_dict.pop(sender_pool.sender_address)
            self._sender_pool_heartbeat_queue.pop(sender_pool)
            LOG.debug(f"Done sender {self._chain_id, sender_pool.sender_address}")
        elif new_state == sender_pool.State.Suspended:
            self._suspended_sender_set.add(NeonAddress.from_raw(sender_pool.sender_address, self._chain_id))
            LOG.debug(f"Suspend sender {self._chain_id, sender_pool.sender_address}")
        elif new_state == sender_pool.State.Queued:
            self._sender_pool_queue.add(sender_pool)
            LOG.debug(f"Include sender {self._chain_id, sender_pool.sender_address} into execution queue")

    def add_tx(self, tx: MPTxRequest) -> MPTxSendResult:
        LOG.debug(
            f"Try to add tx {tx.sig} (gas price {tx.gas_price}, nonce {tx.nonce}) "
            f"to mempool {self.chain_id} with {self.tx_cnt} txs"
        )

        old_tx = self._tx_dict.get_tx_by_hash(tx.sig)
        if old_tx is not None:
            LOG.debug(f"Tx {tx.sig} is already in mempool")
            return MPTxSendResult(code=MPTxSendResultCode.AlreadyKnown, state_tx_cnt=None)

        old_tx = self._tx_dict.get_tx_by_sender_nonce(tx.sender_address, tx.nonce)
        if (old_tx is not None) and (old_tx.gas_price >= tx.gas_price):
            LOG.debug(f"Old tx {old_tx.sig} has higher gas price {old_tx.gas_price} > {tx.gas_price}")
            return MPTxSendResult(code=MPTxSendResultCode.Underprice, state_tx_cnt=None)

        sender_pool = self._get_or_create_sender_pool(tx.sender_address)
        LOG.debug(f"Got sender pool {tx.chain_id, tx.sender_address} with {sender_pool.len_tx_nonce_queue} txs")

        # this condition checks the processing tx too
        state_tx_cnt = max(tx.neon_tx_exec_cfg.state_tx_cnt, sender_pool.state_tx_cnt)
        is_gapped_tx = (sender_pool.pending_nonce or state_tx_cnt) < tx.nonce

        if self.tx_cnt >= self._capacity_high_watermark:
            if is_gapped_tx:
                if (lower_tx := self._tx_dict.peek_gapped_lower_tx()) is None:
                    return MPTxSendResult(code=MPTxSendResultCode.NonceTooHigh, state_tx_cnt=state_tx_cnt)
                elif tx.gas_price < lower_tx.gas_price:
                    LOG.debug(f"Lowermost tx {lower_tx.sig} has higher gas price {lower_tx.gas_price} > {tx.gas_price}")
                    return MPTxSendResult(code=MPTxSendResultCode.Underprice, state_tx_cnt=None)
            elif self.tx_cnt >= self._capacity and self._tx_dict.peek_gapped_lower_tx() is None:
                lower_tx = self._tx_dict.peek_pending_lower_tx()
                if (lower_tx is not None) and (tx.gas_price < lower_tx.gas_price):
                    LOG.debug(f"Lowermost tx {lower_tx.sig} has higher gas price {lower_tx.gas_price} > {tx.gas_price}")
                    return MPTxSendResult(code=MPTxSendResultCode.Underprice, state_tx_cnt=None)

        if sender_pool.state == sender_pool.State.Processing:
            top_tx = sender_pool.top_tx
            if top_tx.nonce == tx.nonce:
                LOG.debug(f"Old tx {top_tx.sig} (gas price {top_tx.gas_price}) is processing")
                return MPTxSendResult(code=MPTxSendResultCode.NonceTooLow, state_tx_cnt=top_tx.nonce + 1)

        if state_tx_cnt > tx.nonce:
            LOG.debug(f"Sender {tx.sender_address} has higher tx counter {state_tx_cnt} > {tx.nonce}")
            return MPTxSendResult(code=MPTxSendResultCode.NonceTooLow, state_tx_cnt=state_tx_cnt)

        # Everything is ok, let's add transaction to the pool
        if old_tx is not None:
            with logging_context(req_id=old_tx.req_id):
                LOG.debug(
                    f"Replace tx {old_tx.sig} (gas price {old_tx.gas_price}) "
                    f"with tx {tx.sig} (gas price {tx.gas_price})"
                )
                self._drop_tx_from_sender_pool(sender_pool, old_tx)

        self._add_tx_to_sender_pool(sender_pool, tx, is_gapped_tx)
        self._schedule_sender_pool(sender_pool, state_tx_cnt)
        self._check_oversized_and_reduce(tx)
        return MPTxSendResult(code=MPTxSendResultCode.Success, state_tx_cnt=None)

    def drop_stuck_tx(self, neon_sig: str) -> bool:
        tx = self._tx_dict.get_tx_by_hash(neon_sig)
        if tx is None:
            return True

        sender_pool = self._get_sender_pool(tx.sender_address)
        if sender_pool.state == sender_pool.State.Processing:
            return False

        self._set_sender_tx_cnt(sender_pool, tx.nonce)
        self._drop_tx_from_sender_pool(sender_pool, tx)
        self._sync_sender_state(sender_pool)
        return True

    @property
    def tx_cnt(self) -> int:
        return len(self._tx_dict)

    @property
    def pending_tx_cnt(self) -> int:
        return self._tx_dict.len_tx_gas_price_queue

    def _check_oversized_and_reduce(self, new_tx: MPTxRequest) -> None:
        tx_cnt_to_remove = self.tx_cnt - self._capacity
        if tx_cnt_to_remove <= 0:
            return

        LOG.debug(f"Try to clear {tx_cnt_to_remove} txs by lower gas price")

        changed_sender_set: Set[str] = set()
        for i in range(tx_cnt_to_remove):
            tx = self._tx_dict.peek_lower_tx()
            if (tx is None) or (tx.sig == new_tx.sig):
                LOG.debug(f"Break on tx {tx}")
                break

            with logging_context(req_id=tx.req_id):
                LOG.debug(f"Remove tx {tx.sig} from {tx.sender_address} pool by lower gas price {tx.gas_price}")
                sender_pool = self._get_sender_pool(tx.sender_address)
                changed_sender_set.add(tx.sender_address)
                self._drop_tx_from_sender_pool(sender_pool, tx)

        for sender_address in changed_sender_set:
            sender_pool = self._get_sender_pool(sender_address)
            self._sync_sender_state(sender_pool)

    def peek_top_tx(self) -> Optional[MPTxRequest]:
        if len(self._sender_pool_queue) == 0:
            return None
        return self._sender_pool_queue[self._top_index].top_tx

    def acquire_tx(self, tx: MPTxRequest) -> Optional[MPTxRequest]:
        sender_pool = self._get_sender_pool(tx.sender_address)
        assert sender_pool.state == sender_pool.State.Queued

        self._sender_pool_queue.pop(sender_pool)
        sender_pool.acquire_tx(tx)
        self._tx_dict.acquire_tx(tx)
        return tx

    def get_pending_tx_nonce(self, sender_address: str) -> Optional[int]:
        sender_pool = self._find_sender_pool(sender_address)
        return None if sender_pool is None else sender_pool.pending_nonce

    def get_last_tx_nonce(self, sender_address: str) -> Optional[int]:
        sender_pool = self._find_sender_pool(sender_address)
        return None if sender_pool is None else sender_pool.last_nonce

    def get_pending_tx_by_hash(self, neon_sig: str) -> Optional[NeonTxInfo]:
        tx = self._tx_dict.get_tx_by_hash(neon_sig)
        return None if tx is None else tx.neon_tx_info

    def get_pending_tx_by_sender_nonce(self, sender_addr: str, tx_nonce: int) -> Optional[NeonTxInfo]:
        tx = self._tx_dict.get_tx_by_sender_nonce(sender_addr, tx_nonce)
        return None if tx is None else tx.neon_tx_info

    def _done_tx(self, tx: MPTxRequest) -> None:
        LOG.debug(f"Done tx {tx.sig} in pool {tx.sender_address}")

        sender_pool = self._get_sender_pool(tx.sender_address)
        sender_pool.done_tx(tx)
        self._schedule_sender_pool(sender_pool, tx.neon_tx_exec_cfg.state_tx_cnt)
        self._tx_dict.done_tx(tx, sender_pool.state == sender_pool.State.Suspended)

    def done_tx(self, tx: MPTxRequest) -> None:
        self._done_tx(tx)

    def fail_tx(self, tx: MPTxRequest) -> None:
        self._done_tx(tx)

    def cancel_tx(self, tx: MPTxRequest) -> bool:
        sender_pool = self._get_sender_pool(tx.sender_address)
        sender_pool.cancel_process_tx(tx)
        self._schedule_sender_pool(sender_pool, tx.neon_tx_exec_cfg.state_tx_cnt)
        self._tx_dict.cancel_process_tx(tx, sender_pool.state == sender_pool.State.Suspended)
        return True

    @property
    def suspended_sender_list(self) -> List[NeonAddress]:
        return list(self._suspended_sender_set)

    def set_sender_state_tx_cnt(self, sender_tx_cnt: MPSenderTxCntData) -> None:
        sender_pool = self._find_sender_pool(sender_tx_cnt.sender.address)
        if sender_pool and sender_pool.state == sender_pool.State.Suspended:
            self._schedule_sender_pool(sender_pool, sender_tx_cnt.state_tx_cnt)
            if sender_pool.state == sender_pool.State.Queued:
                self._tx_dict.queue_tx(sender_pool.sender_address, sender_pool.top_tx.nonce)

    def get_content(self) -> MPTxPoolContentResult:
        pending_list: List[NeonTxInfo] = list()
        queued_list: List[NeonTxInfo] = list()

        for tx_pool in self._sender_pool_dict.values():
            tx_list = tx_pool.tx_list()
            pending_stop_pos = tx_pool.pending_stop_pos
            pending_list.extend([tx.neon_tx_info for tx in tx_list[:pending_stop_pos]])
            queued_list.extend([tx.neon_tx_info for tx in tx_list[pending_stop_pos:]])

        return MPTxPoolContentResult(pending_list=pending_list, queued_list=queued_list)
