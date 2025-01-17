import json
import logging
import requests
import time
import enum

from typing import Optional, Dict, Any, Union, List

from .neon_client import NeonClient
from .neon_client_base import NeonClientBase
from .neon_layouts import NeonAccountInfo, NeonContractInfo, EVMConfigInfo, HolderAccountInfo

from ..common_neon.address import NeonAddress
from ..common_neon.config import Config
from ..common_neon.data import NeonEmulatorResult, NeonEmulatorExitStatus, SolanaOverrides
from ..common_neon.errors import EthereumError
from ..common_neon.solana_block import SolBlockInfo
from ..common_neon.solana_tx import SolCommit, SolPubKey
from ..common_neon.utils.eth_proto import NeonTx
from ..common_neon.utils.utils import cached_property
from ..common_neon.evm_config import EVMConfig


LOG = logging.getLogger(__name__)
RPCRequest = Dict[str, Any]
RPCResponse = Dict[str, Any]


class _MethodName(enum.Enum):
    emulate = 'emulate'
    get_storage_at = 'storage'
    get_neon_account_info_list = 'balance'
    get_neon_contract_info = 'contract'
    get_config = 'config'
    get_version = 'build-info'
    get_holder_info = 'holder'


class _Client:
    def __init__(self, port: int):
        self._port = port

        self._headers = {
            'Content-Type': 'application/json',
        }

        base_url = f'http://127.0.0.1:{port}/api'
        self._call_url_map = {
            method: base_url + '/' + method.value
            for method in list(_MethodName)
        }

    def __del__(self):
        self._close()

    @property
    def port(self) -> int:
        return self._port

    @cached_property
    def _client(self) -> requests.Session:
        client = requests.Session()
        client.headers = self._headers
        return client

    def _close(self) -> None:
        self._client.close()

    def _get_json(self, raw_response: requests.Response) -> RPCResponse:
        json_response = raw_response.json()
        error = json_response.get('error', None)
        if error is None:
            return json_response

        if error.startswith('Solana client error.'):
            raise ValueError(raw_response.content)
        return json_response

    def call(self, method: _MethodName, request: RPCRequest) -> RPCResponse:
        raw_response: Optional[requests.Response] = None
        try:
            raw_response = self._client.post(self._call_url_map[method], json=request)
            return self._get_json(raw_response)
        except (BaseException,):
            self._close()
            if raw_response is not None:
                raise ValueError(raw_response.content)
            raise


class NeonCoreApiClient(NeonClientBase):
    def __init__(self, config: Config):
        self._config = config
        self._client_cnt = len(config.solana_url_list)

        port = config.neon_core_api_port
        self._client_list = [_Client(port + idx) for idx in range(self._client_cnt)]
        self._last_client_idx = 0

    def _get_client(self) -> _Client:
        idx = self._last_client_idx

        self._last_client_idx += 1
        if self._last_client_idx >= len(self._client_list):
            self._last_client_idx = 0

        return self._client_list[idx]

    def _call(self, method: _MethodName, request: RPCRequest) -> RPCResponse:
        for retry in range(30):
            for _ in range(self._client_cnt):
                client = self._get_client()
                try:
                    return client.call(method, request)
                except BaseException as exc:
                    LOG.warning(f'Fail to call {method} on the neon_core_api({client.port})', exc_info=exc)
            LOG.warning(f'Fail to call {method} on the neon_core_api, sleep on 1 second...')
            time.sleep(1)

    def emulate(
        self, contract: Optional[NeonAddress],
        sender: Optional[NeonAddress],
        chain_id: int,
        data: Optional[str],
        value: Optional[Union[str, int]],
        gas_limit: Optional[str] = None,
        block: Optional[SolBlockInfo] = None,
        check_result=False,
        solana_overrides: Optional[SolanaOverrides] = None
    ) -> NeonEmulatorResult:
        if not sender:
            sender = '0x0000000000000000000000000000000000000000'
        else:
            sender = sender.address

        if contract:
            contract = contract.address

        if data is not None:
            if data[:2] in {'0x', '0X'}:
                data = data[2:]
            try:
                hex_data = bytes.fromhex(data).hex()
                assert len(hex_data) == len(data)
            except (BaseException, ):
                raise EthereumError('Invalid data')

        if not value:
            value = '0x0'
        elif isinstance(value, int):
            value = hex(value)

        if isinstance(gas_limit, int):
            gas_limit = hex(value)

        request = dict(
            step_limit=self._config.max_evm_step_cnt_emulate,
            accounts=[],
            chains=EVMConfig().chain_json_list,
            tx={
                'from': sender,
                'to': contract,
                'value': value,
                'data': data,
                'chain_id': chain_id,

                # 'nonce': None,
                # 'gas_limit': gas_limit,
                # 'access_list': None
            },
            solana_overrides=solana_overrides
        )
        request = self._add_block(request, block)
        response = self._call(_MethodName.emulate, request)
        self._check_emulated_error(response)

        if check_result:
            return self._get_emulated_result(response)
        return NeonEmulatorResult(response.get('value'))

    def emulate_neon_tx(self, neon_tx: NeonTx, chain_id: int, solana_overrides: Optional[SolanaOverrides] = None) -> NeonEmulatorResult:
        return self.emulate(
            NeonAddress.from_raw(neon_tx.toAddress, chain_id),
            NeonAddress.from_raw(neon_tx.sender, chain_id),
            chain_id,
            neon_tx.hex_call_data,
            neon_tx.value,
            solana_overrides=solana_overrides,
        )

    def get_storage_at(self, contract: NeonAddress, position: str, block: SolBlockInfo) -> str:
        request = dict(
            contract=contract.address,
            index=position
        )
        request = self._add_block(request, block)
        response = self._call(_MethodName.get_storage_at, request)
        value = response.get('value')
        if value is None:
            raise EthereumError('No storage')
        return '0x' + bytes(value).hex()

    def get_neon_account_info_list(
        self, addr_list: List[NeonAddress],
        block: Optional[SolBlockInfo] = None
    ) -> List[NeonAccountInfo]:
        request = dict(
            account=[
                dict(
                    address=addr.address,
                    chain_id=addr.chain_id
                )
                for addr in addr_list
            ]
        )
        request = self._add_block(request, block)

        response = self._call(_MethodName.get_neon_account_info_list, request)
        json_acct_list = response.get('value')
        return [
            NeonAccountInfo.from_json(addr, json_acct)
            for addr, json_acct in zip(addr_list, json_acct_list)
        ]

    def get_neon_account_info(
        self, addr: NeonAddress,
        block: Optional[SolBlockInfo] = None
    ) -> NeonAccountInfo:
        return self.get_neon_account_info_list([addr], block)[0]

    def get_neon_contract_info(
        self, addr: NeonAddress,
        block: Optional[SolBlockInfo] = None
    ) -> Optional[NeonContractInfo]:
        request = dict(
            contract=addr.address
        )
        request = self._add_block(request, block)

        response = self._call(_MethodName.get_neon_contract_info, request)
        json_contract = response.get('value')[0]
        return NeonContractInfo.from_json(addr, json_contract)

    def get_state_tx_cnt(
        self, addr: Union[NeonAddress, NeonAccountInfo],
        block: Optional[SolBlockInfo] = None
    ) -> int:
        if not isinstance(addr, NeonAccountInfo):
            neon_acct_info = self.get_neon_account_info(addr, block)
        else:
            neon_acct_info = addr
        return neon_acct_info.tx_count

    def get_evm_config(self, last_deployed_slot: int) -> EVMConfigInfo:
        response = self._call(_MethodName.get_config, dict())
        json_cfg = response.get('value')
        return EVMConfigInfo.from_json(last_deployed_slot, self._config, json_cfg)

    def get_holder_account_info(self, addr: SolPubKey) -> HolderAccountInfo:
        request = dict(pubkey=str(addr))
        response = self._call(_MethodName.get_holder_info, request)
        json_acct = response.get('value')
        return HolderAccountInfo.from_json(addr, json_acct)

    def version(self) -> str:
        return NeonClient(self._config).version()

    def _add_block(self, request: RPCRequest, block: Optional[SolBlockInfo]) -> RPCRequest:
        if not block:
            pass
        elif block.sol_commit in {SolCommit.Confirmed, SolCommit.Processed}:
            pass
        elif len(self._config.ch_dsn_list):
            request.update(dict(slot=block.block_slot))
        else:
            request.update(dict(commitment=block.sol_commit))
        return request

    def _check_emulated_error(self, response: RPCResponse) -> None:
        error = response.get('error')
        if error is None:
            return

        raise EthereumError(message=error)

    def _get_emulated_result(self, response: RPCResponse) -> NeonEmulatorResult:
        value = response.get('value')
        if value is None:
            raise EthereumError(message=json.dumps(response))

        emulated_result = NeonEmulatorResult(value)
        exit_status = emulated_result.exit_status
        if exit_status == NeonEmulatorExitStatus.Revert:
            revert_data = emulated_result.revert_data
            LOG.debug(f'Got revert call emulated result with data: {revert_data}')
            result_value = self._decode_revert_message(revert_data)
            if result_value is None:
                raise EthereumError(code=3, message='execution reverted', data='0x' + revert_data)
            else:
                raise EthereumError(code=3, message='execution reverted: ' + result_value, data='0x' + revert_data)

        if exit_status != NeonEmulatorExitStatus.Succeed:
            LOG.debug(f'Got not succeed emulate exit_status: {exit_status}')
            reason = emulated_result.exit_reason
            if isinstance(reason, str):
                raise EthereumError(code=3, message=f'execution finished with error: {reason}')
            raise EthereumError(code=3, message=exit_status)

        return emulated_result

    @staticmethod
    def _decode_revert_message(data: str) -> Optional[str]:
        data_len = len(data)
        if data_len == 0:
            return None

        if data_len < 8:
            raise Exception(f'Too less bytes to decode revert signature: {data_len}, data: 0x{data}')

        if data[:8] == '4e487b71':  # keccak256("Panic(uint256)")
            return None

        if data[:8] != '08c379a0':  # keccak256("Error(string)")
            LOG.debug(f'Failed to decode revert_message, unknown revert signature: {data[:8]}')
            return None

        if data_len < 8 + 64:
            raise Exception(f'Too less bytes to decode revert msg offset: {data_len}, data: 0x{data}')
        offset = int(data[8:8 + 64], 16) * 2

        if data_len < 8 + offset + 64:
            raise Exception(f'Too less bytes to decode revert msg len: {data_len}, data: 0x{data}')
        length = int(data[8 + offset:8 + offset + 64], 16) * 2

        if data_len < 8 + offset + 64 + length:
            raise Exception(f'Too less bytes to decode revert msg: {data_len}, data: 0x{data}')

        message = str(bytes.fromhex(data[8 + offset + 64:8 + offset + 64 + length]), 'utf8')
        return message
