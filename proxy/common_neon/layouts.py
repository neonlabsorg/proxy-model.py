from __future__ import annotations

import math
import logging

from dataclasses import dataclass
from typing import Optional, List

from construct import Bytes, Int8ul, Int16ul, Int32ul, Int64ul
from construct import Struct

from .constants import (
    LOOKUP_ACCOUNT_TAG, ADDRESS_LOOKUP_TABLE_ID,
    NEON_LEGACY_ACCOUNT_TAG, NEON_BALANCE_TAG, NEON_CONTRACT_TAG, NEON_STORAGE_CELL_TAG
)

from .address import NeonAddress
from .solana_tx import SolPubKey


LOG = logging.getLogger(__name__)


HOLDER_ACCOUNT_INFO_LAYOUT = Struct(
    "tag" / Int8ul,
    "operator" / Bytes(32),
    "neon_tx_sig" / Bytes(32)
)

ACTIVE_HOLDER_ACCOUNT_INFO_LAYOUT = Struct(
    "tag" / Int8ul,
    "operator" / Bytes(32),
    "neon_tx_sig" / Bytes(32),
    "caller" / Bytes(20),
    "gas_limit" / Bytes(32),
    "gas_price" / Bytes(32),
    "gas_used" / Bytes(32),
    "last_operator" / Bytes(32),
    "block_slot" / Int64ul,
    "account_list_len" / Int64ul,
    "evm_state_len" / Int64ul,
    "evm_machine_len" / Int64ul,
)

FINALIZED_HOLDER_ACCOUNT_INFO_LAYOUT = Struct(
    "tag" / Int8ul,
    "operator" / Bytes(32),
    "neon_tx_sig" / Bytes(32)
)

NEON_LEGACY_ACCOUNT_LAYOUT = Struct(
    "type" / Int8ul,
    "neon_address" / Bytes(20),
    "nonce" / Int8ul,
    "tx_count" / Bytes(8),
    "balance" / Bytes(32),
    "generation" / Int32ul,
    "code_size" / Int32ul,
    "is_rw_blocked" / Int8ul,
)

NEON_ACCOUNT_LAYOUT = Struct(
    "type" / Int8ul,
    "is_rw_blocked" / Int8ul,
    "neon_address" / Bytes(20),
    "chain_id" / Int64ul
)

NEON_STORAGE_CELL_LAYOUT = Struct(
    "type" / Int8ul,
    "is_rw_blocked" / Int8ul,
)

ACCOUNT_LOOKUP_TABLE_LAYOUT = Struct(
    "type" / Int32ul,
    "deactivation_slot" / Int64ul,
    "last_extended_slot" / Int64ul,
    "last_extended_slot_start_index" / Int8ul,
    "has_authority" / Int8ul,
    "authority" / Bytes(32),
    "padding" / Int16ul
)


@dataclass
class AccountInfo:
    address: SolPubKey
    tag: int
    lamports: int
    owner: SolPubKey
    data: bytes


@dataclass
class NeonAccountInfo:
    pda_address: SolPubKey
    neon_address: Optional[NeonAddress]
    is_rw_blocked: bool

    @staticmethod
    def from_account_info(info: AccountInfo) -> Optional[NeonAccountInfo]:
        if info.tag == NEON_LEGACY_ACCOUNT_TAG:
            return NeonAccountInfo._from_legacy_account_info(info)
        elif info.tag in (NEON_BALANCE_TAG, NEON_CONTRACT_TAG):
            return NeonAccountInfo._from_account_info(info)
        elif info.tag == NEON_STORAGE_CELL_TAG:
            return NeonAccountInfo._from_storage_cell_info(info)
        return None

    @staticmethod
    def _from_legacy_account_info(info: AccountInfo) -> NeonAccountInfo:
        assert info.tag == NEON_LEGACY_ACCOUNT_TAG
        cont = NeonAccountInfo._extract_cont(NEON_LEGACY_ACCOUNT_LAYOUT, info)
        return NeonAccountInfo(
            pda_address=info.address,
            neon_address=NeonAddress(cont.neon_address, None),
            is_rw_blocked=(cont.is_rw_blocked != 0),
        )

    @staticmethod
    def _from_account_info(info: AccountInfo) -> NeonAccountInfo:
        assert info.tag in (NEON_BALANCE_TAG, NEON_CONTRACT_TAG)
        cont = NeonAccountInfo._extract_cont(NEON_ACCOUNT_LAYOUT, info)
        return NeonAccountInfo(
            pda_address=info.address,
            neon_address=NeonAddress(cont.neon_address, cont.chain_id),
            is_rw_blocked=(cont.is_rw_blocked == 1),
        )

    @staticmethod
    def _from_storage_cell_info(info: AccountInfo) -> NeonAccountInfo:
        assert info.tag == NEON_STORAGE_CELL_TAG
        cont = NeonAccountInfo._extract_cont(NEON_STORAGE_CELL_LAYOUT, info)
        return NeonAccountInfo(
            pda_address=info.address,
            neon_address=None,
            is_rw_blocked=(cont.is_rw_blocked == 1),
        )

    @staticmethod
    def _extract_cont(layout: Struct, info: AccountInfo):
        min_size = layout.sizeof()
        if len(info.data) < min_size:
            raise RuntimeError(
                f'Wrong data length for account data {str(info.address)}({info.tag}): '
                f'{len(info.data)} < {min_size}'
            )
        return layout.parse(info.data)

    @staticmethod
    def min_size() -> int:
        return max(
            NEON_LEGACY_ACCOUNT_LAYOUT.sizeof(),
            NEON_ACCOUNT_LAYOUT.sizeof(),
            NEON_STORAGE_CELL_LAYOUT.sizeof()
        )


@dataclass
class ALTAccountInfo:
    type: int
    table_account: SolPubKey
    deactivation_slot: Optional[int]
    last_extended_slot: int
    last_extended_slot_start_index: int
    authority: Optional[SolPubKey]
    account_key_list: List[SolPubKey]

    @staticmethod
    def from_account_info(info: AccountInfo) -> Optional[ALTAccountInfo]:
        if info.owner != ADDRESS_LOOKUP_TABLE_ID:
            LOG.warning(f'Wrong owner {str(info.owner)} of account {str(info.address)}')
            return None
        elif len(info.data) < ACCOUNT_LOOKUP_TABLE_LAYOUT.sizeof():
            LOG.warning(
                f'Wrong data length for lookup table data {str(info.address)}: '
                f'{len(info.data)} < {ACCOUNT_LOOKUP_TABLE_LAYOUT.sizeof()}'
            )
            return None

        lookup = ACCOUNT_LOOKUP_TABLE_LAYOUT.parse(info.data)
        if lookup.type != LOOKUP_ACCOUNT_TAG:
            return None

        offset = ACCOUNT_LOOKUP_TABLE_LAYOUT.sizeof()
        if (len(info.data) - offset) % SolPubKey.LENGTH:
            return None

        account_key_list = []
        account_key_list_len = math.ceil((len(info.data) - offset) / SolPubKey.LENGTH)
        for _ in range(account_key_list_len):
            some_pubkey = SolPubKey.from_bytes(info.data[offset:offset + SolPubKey.LENGTH])
            offset += SolPubKey.LENGTH
            account_key_list.append(some_pubkey)

        authority = SolPubKey.from_bytes(lookup.authority) if lookup.has_authority else None

        u64_max = 2 ** 64 - 1

        return ALTAccountInfo(
            type=lookup.type,
            table_account=info.address,
            deactivation_slot=None if lookup.deactivation_slot == u64_max else lookup.deactivation_slot,
            last_extended_slot=lookup.last_extended_slot,
            last_extended_slot_start_index=lookup.last_extended_slot_start_index,
            authority=authority,
            account_key_list=account_key_list
        )
