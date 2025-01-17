from __future__ import annotations

import asyncio
import time

from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Optional, List, Union

from ..common_neon.data import NeonTxExecCfg
from ..common_neon.operator_resource_info import OpResInfo
from ..common_neon.errors import EthereumError
from ..common_neon.solana_tx import SolPubKey
from ..common_neon.utils import str_fmt_object
from ..common_neon.utils.eth_proto import NeonTx
from ..common_neon.utils.neon_tx_info import NeonTxInfo
from ..common_neon.address import NeonAddress

from ..neon_core_api.neon_layouts import EVMConfigInfo


@dataclass(frozen=True)
class MPTask:
    executor_id: int
    aio_task: asyncio.Task
    mp_request: MPRequest


class MPRequestType(IntEnum):
    SendTransaction = 0
    GetPendingTxNonce = 1
    GetMempoolTxNonce = 2
    GetTxByHash = 3
    GetGasPrice = 4
    GetStateTxCnt = 5
    GetOperatorResourceList = 6
    InitOperatorResource = 7
    GetEVMConfig = 8
    GetALTList = 9
    DeactivateALTList = 10
    CloseALTList = 11
    GetStuckTxList = 12
    TxPoolContent = 13
    GetTxBySenderNonce = 14
    Unspecified = 255


@dataclass
class MPRequest:
    req_id: str
    type: MPRequestType = MPRequestType.Unspecified

    def __str__(self) -> str:
        return str_fmt_object(self)


@dataclass(frozen=True)
class MPStuckTxInfo:
    neon_tx: NeonTxInfo
    holder_account: SolPubKey
    alt_addr_list: List[str]
    start_time: int
    chain_id: Optional[int] = None

    def __post_init__(self):
        if self.neon_tx.has_chain_id():
            self.set_chain_id(self.neon_tx.chain_id)

    def __str__(self) -> str:
        return str_fmt_object(self)

    @property
    def sig(self) -> str:
        return self.neon_tx.sig

    @property
    def req_id(self) -> str:
        return self.neon_tx.sig[2:10]

    def has_chain_id(self) -> bool:
        return self.chain_id is not None

    def set_chain_id(self, chain_id: int) -> None:
        object.__setattr__(self, "chain_id", chain_id)


@dataclass(frozen=True)
class MPGetStuckTxListResponse:
    stuck_tx_list: List[MPStuckTxInfo]


@dataclass
class MPTxRequest(MPRequest):
    neon_tx: Optional[NeonTx] = None
    neon_tx_info: Optional[NeonTxInfo] = None
    neon_tx_exec_cfg: Optional[NeonTxExecCfg] = None
    chain_id: int = 0
    gas_price: int = 0
    start_time: int = 0

    @staticmethod
    def from_neon_tx(req_id: str, neon_tx: NeonTx, def_chain_id: int, neon_tx_exec_cfg: NeonTxExecCfg) -> MPTxRequest:
        neon_tx_info = NeonTxInfo.from_neon_tx(neon_tx)
        chain_id = neon_tx_info.chain_id or def_chain_id
        return MPTxRequest(
            req_id=req_id,
            neon_tx=neon_tx,
            neon_tx_info=neon_tx_info,
            neon_tx_exec_cfg=neon_tx_exec_cfg,
            chain_id=chain_id,
            gas_price=neon_tx.gasPrice,
            start_time=time.time_ns(),
        )

    def __post_init__(self):
        self.type = MPRequestType.SendTransaction

    @property
    def sig(self) -> str:
        return self.neon_tx_info.sig

    @property
    def sender_address(self) -> str:
        return self.neon_tx_info.addr

    @property
    def nonce(self) -> int:
        return self.neon_tx_info.nonce

    def has_chain_id(self) -> bool:
        return self.neon_tx_info.has_chain_id()


@dataclass
class MPTxExecRequest(MPTxRequest):
    evm_config_data: EVMConfigInfo = None
    res_info: OpResInfo = None

    def is_stuck_tx(self) -> bool:
        return self.neon_tx is None

    @staticmethod
    def from_tx_req(tx: MPTxRequest, res_info: OpResInfo, evm_config_data: EVMConfigInfo) -> MPTxExecRequest:
        return MPTxExecRequest(
            req_id=tx.req_id,
            neon_tx=tx.neon_tx,
            neon_tx_info=tx.neon_tx_info,
            neon_tx_exec_cfg=tx.neon_tx_exec_cfg,
            chain_id=tx.chain_id,
            gas_price=tx.gas_price,
            start_time=tx.start_time,
            evm_config_data=evm_config_data,
            res_info=res_info,
        )

    @staticmethod
    def from_stuck_tx(
        stuck_tx: MPStuckTxInfo, neon_tx_exec_cfg: NeonTxExecCfg, res_info: OpResInfo, evm_config_data: EVMConfigInfo
    ) -> MPTxExecRequest:
        return MPTxExecRequest(
            req_id=stuck_tx.req_id,
            neon_tx=None,
            neon_tx_info=stuck_tx.neon_tx,
            neon_tx_exec_cfg=neon_tx_exec_cfg,
            chain_id=stuck_tx.chain_id,
            gas_price=stuck_tx.neon_tx.gas_price,
            start_time=stuck_tx.start_time,
            evm_config_data=evm_config_data,
            res_info=res_info,
        )


MPTxRequestList = List[MPTxRequest]


@dataclass
class MPPendingTxNonceRequest(MPRequest):
    sender: NeonAddress = None

    def __post_init__(self):
        self.type = MPRequestType.GetPendingTxNonce


@dataclass
class MPMempoolTxNonceRequest(MPRequest):
    sender: NeonAddress = None

    def __post_init__(self):
        self.type = MPRequestType.GetMempoolTxNonce


@dataclass
class MPPendingTxByHashRequest(MPRequest):
    tx_hash: str = None

    def __post_init__(self):
        self.type = MPRequestType.GetTxByHash


@dataclass
class MPPendingTxBySenderNonceRequest(MPRequest):
    sender: NeonAddress = None
    tx_nonce: int = 0

    def __post_init__(self):
        self.type = MPRequestType.GetTxBySenderNonce


@dataclass(frozen=True)
class MPGasPriceTokenRequest:
    chain_id: int
    token_name: str
    price_account: Optional[SolPubKey] = None


@dataclass
class MPGasPriceRequest(MPRequest):
    last_update_mapping_sec: int = 0

    sol_price_account: Optional[SolPubKey] = None
    token_list: List[MPGasPriceTokenRequest] = None

    def __post_init__(self):
        self.type = MPRequestType.GetGasPrice
        if not self.token_list:
            self.token_list = list()


@dataclass
class MPGetEVMConfigRequest(MPRequest):
    evm_config_data: EVMConfigInfo = None

    def __post_init__(self):
        self.type = MPRequestType.GetEVMConfig


@dataclass
class MPSenderTxCntRequest(MPRequest):
    sender_list: List[NeonAddress] = None

    def __post_init__(self):
        self.type = MPRequestType.GetStateTxCnt


@dataclass
class MPOpResGetListRequest(MPRequest):
    evm_config_data: EVMConfigInfo = None

    def __post_init__(self):
        self.type = MPRequestType.GetOperatorResourceList


@dataclass
class MPOpResInitRequest(MPRequest):
    evm_config_data: EVMConfigInfo = None
    res_info: OpResInfo = None

    def __post_init__(self):
        self.type = MPRequestType.InitOperatorResource


@dataclass
class MPALTAddress:
    table_account: str
    secret: bytes


@dataclass
class MPGetALTList(MPRequest):
    secret_list: List[bytes] = None
    alt_address_list: List[MPALTAddress] = None

    def __post_init__(self):
        self.type = MPRequestType.GetALTList


@dataclass
class MPALTInfo:
    last_extended_slot: int
    deactivation_slot: Optional[int]
    block_height: int
    table_account: str
    operator_key: bytes

    def is_deactivated(self) -> bool:
        return self.deactivation_slot is not None


@dataclass
class MPDeactivateALTListRequest(MPRequest):
    alt_info_list: List[MPALTInfo] = None

    def __post_init__(self):
        self.type = MPRequestType.DeactivateALTList


@dataclass
class MPCloseALTListRequest(MPRequest):
    alt_info_list: List[MPALTInfo] = None

    def __post_init__(self):
        self.type = MPRequestType.CloseALTList


@dataclass
class MPGetStuckTxListRequest(MPRequest):
    def __post_init__(self):
        self.type = MPRequestType.GetStuckTxList


@dataclass
class MPTxPoolContentRequest(MPRequest):
    def __post_init__(self):
        self.type = MPRequestType.TxPoolContent


class MPTxExecResultCode(IntEnum):
    Done = 0
    Reschedule = 1
    Failed = 2
    BadResource = 3
    NonceTooHigh = 4
    StuckTx = 5


@dataclass(frozen=True)
class MPTxExecResult:
    code: MPTxExecResultCode
    data: Any

    def __str__(self) -> str:
        return str_fmt_object(self)


class MPTxSendResultCode(IntEnum):
    Success = 0
    NonceTooLow = 1
    Underprice = 2
    AlreadyKnown = 3
    NonceTooHigh = 4
    Unspecified = 255


@dataclass(frozen=True)
class MPTxSendResult:
    code: MPTxSendResultCode
    state_tx_cnt: Optional[int]


@dataclass(frozen=True)
class MPGasPriceTokenResult:
    chain_id: int
    token_name: str
    token_price_usd: int

    operator_fee: int
    gas_price_slippage: int
    cu_priority_fee: int
    simple_cu_priority_fee: int

    suggested_gas_price: int
    is_const_gas_price: bool
    min_acceptable_gas_price: int
    min_executable_gas_price: int

    min_wo_chainid_acceptable_gas_price: int
    allow_underpriced_tx_wo_chainid: bool
    token_price_account: SolPubKey

    is_overloaded: bool = False

    def up_min_executable_gas_price(self, min_executable_gas_price: int) -> None:
        object.__setattr__(self, "min_executable_gas_price", min_executable_gas_price)

    def up_suggested_gas_price(self, min_gas_price: int) -> None:
        if self.suggested_gas_price < min_gas_price:
            object.__setattr__(self, "suggested_gas_price", min_gas_price)
            object.__setattr__(self, "is_overloaded", True)


@dataclass(frozen=True)
class MPGasPriceResult:
    sol_price_usd: int
    sol_price_account: SolPubKey
    last_update_mapping_sec: int
    token_list: List[MPGasPriceTokenResult] = None


MPEVMConfigResult = EVMConfigInfo


@dataclass(frozen=True)
class MPSenderTxCntData:
    sender: NeonAddress
    state_tx_cnt: int


@dataclass(frozen=True)
class MPSenderTxCntResult:
    sender_tx_cnt_list: List[MPSenderTxCntData]


class MPOpResInitResultCode(IntEnum):
    Success = 0
    Failed = 1
    Reschedule = 2
    StuckTx = 3


@dataclass(frozen=True)
class MPOpResGetListResult:
    res_info_list: List[OpResInfo]


@dataclass(frozen=True)
class MPOpResInitResult:
    code: MPOpResInitResultCode
    exc: Optional[BaseException]


@dataclass(frozen=True)
class MPALTListResult:
    block_height: int
    alt_info_list: List[MPALTInfo]


MPNeonTxResult = Union[NeonTxInfo, EthereumError, None]


@dataclass(frozen=True)
class MPTxPoolContentResult:
    pending_list: List[NeonTxInfo]
    queued_list: List[NeonTxInfo]

    def extend(self, src: MPTxPoolContentResult):
        self.pending_list.extend(src.pending_list)
        self.queued_list.extend(src.queued_list)
