import logging
import math

from typing import Dict, Any, List, Optional

from ..common_neon.evm_config import EVMConfig
from ..common_neon.data import NeonEmulatorResult, SolanaOverrides
from ..common_neon.utils.eth_proto import NeonTx
from ..common_neon.neon_instruction import NeonIxBuilder
from ..common_neon.solana_alt_limit import ALTLimit
from ..common_neon.solana_tx import SolAccount, SolPubKey, SolAccountMeta, SolBlockHash, SolTxSizeError
from ..common_neon.solana_tx_legacy import SolLegacyTx
from ..common_neon.solana_block import SolBlockInfo
from ..common_neon.address import NeonAddress
from ..common_neon.config import Config

from ..neon_core_api.neon_core_api_client import NeonCoreApiClient

LOG = logging.getLogger(__name__)


class _GasTxBuilder:
    def __init__(self):
        # This values doesn't used on real network, they are used only to generate temporary data
        holder_key = bytes([
            61, 147, 166, 57, 23, 88, 41, 136, 224, 223, 120, 142, 155, 123, 221, 134,
            16, 102, 170, 82, 76, 94, 95, 178, 125, 232, 191, 172, 103, 157, 145, 190
        ])
        holder = SolAccount.from_seed(holder_key)

        operator_key = bytes([
            161, 247, 66, 157, 203, 188, 141, 236, 124, 123, 200, 192, 255, 23, 161, 34,
            116, 202, 70, 182, 176, 194, 195, 168, 185, 132, 161, 142, 203, 57, 245, 90
        ])
        self._signer = SolAccount.from_seed(operator_key)
        self._block_hash = SolBlockHash.from_string('4NCYB3kRT8sCNodPNuCZo8VUh4xqpBQxsxed2wd9xaD4')

        self._neon_ix_builder = NeonIxBuilder(self._signer.pubkey())
        self._neon_ix_builder.init_iterative(holder.pubkey())
        self._neon_ix_builder.init_operator_neon(SolPubKey.default())

    def build_tx(self, config: Config, tx: NeonTx, account_list: List[SolAccountMeta]) -> SolLegacyTx:
        self._neon_ix_builder.init_neon_tx(tx)
        self._neon_ix_builder.init_neon_account_list(account_list)

        ix_list = [
            self._neon_ix_builder.make_compute_budget_heap_ix(),
            self._neon_ix_builder.make_compute_budget_cu_ix(config.cu_limit)
        ]
        if config.cu_priority_fee > 0:
            ix_list.append(self._neon_ix_builder.make_compute_budget_cu_fee_ix(config.cu_priority_fee))

        ix_list.append(self._neon_ix_builder.make_tx_step_from_data_ix(EVMConfig().neon_evm_steps, 1))

        tx = SolLegacyTx(name='Estimate', ix_list=ix_list)

        tx.recent_block_hash = self._block_hash
        tx.sign(self._signer)
        return tx

    @property
    def len_neon_tx(self) -> int:
        return len(self._neon_ix_builder.holder_msg)


class GasEstimate:
    _small_gas_limit = 30_000  # openzeppelin size check
    _tx_builder = _GasTxBuilder()
    _u256_max = int.from_bytes(bytes([0xFF] * 32), 'big')

    def __init__(self, config: Config, core_api_client: NeonCoreApiClient, def_chain_id: int):
        self._config = config

        self._sender: Optional[NeonAddress] = None
        self._contract: Optional[NeonAddress] = None
        self._data: Optional[str] = None
        self._value: Optional[str] = None
        self._gas: Optional[str] = None
        self._gas_price: Optional[str] = None
        self._core_api_client = core_api_client

        self._def_chain_id = def_chain_id

        self._account_list: List[SolAccountMeta] = list()
        self._emulator_result = NeonEmulatorResult()

    def _get_request_param(self, request: Dict[str, Any]) -> None:
        self._sender: Optional[NeonAddress] = request.get('from')
        self._contract: Optional[NeonAddress] = request.get('to')
        self._data: Optional[str] = request.get('data')

        def _get_hex(_key: str) -> Optional[str]:
            _value = request.get(_key, None)
            if isinstance(_value, int):
                _value = hex(_value)
            return _value

        self._value: Optional[str] = _get_hex('value') or '0x0'
        self._gas: Optional[str] = _get_hex('gas') or hex(self._u256_max)
        self._gas_price: Optional[str] = _get_hex('gasPrice')

    def _execute(self, block: SolBlockInfo, solana_overrides: Optional[SolanaOverrides] = None) -> None:
        self._emulator_result = self._core_api_client.emulate(
            self._contract, self._sender, self._def_chain_id, self._data, self._value,
            gas_limit=self._gas, block=block, check_result=True, solana_overrides=solana_overrides,
        )

    def _tx_size_cost(self) -> int:
        to_addr = self._contract.to_bytes() if self._contract else bytes()
        data = bytes.fromhex((self._data or '0x')[2:])
        value = int(self._value, 16)
        gas = int(self._gas, 16)

        if (not value) and (not len(data)):
            value = 1

        neon_tx = NeonTx(
            nonce=self._u256_max,
            gasPrice=self._u256_max,
            gasLimit=gas,
            toAddress=to_addr,
            value=value,
            callData=data,
            v=245022934 * 1024 + 35,
            r=0x1820182018201820182018201820182018201820182018201820182018201820,
            s=0x1820182018201820182018201820182018201820182018201820182018201820
        )

        try:
            sol_tx = self._tx_builder.build_tx(self._config, neon_tx, self._account_list)
            sol_tx.serialize()  # <- there will be exception about size

            if not self._contract:  # deploy case
                pass
            elif self._execution_cost() < self._small_gas_limit:
                return 0
        except SolTxSizeError:
            pass
        except BaseException as exc:
            LOG.debug('Error during pack solana tx', exc_info=exc)

        return self._holder_tx_cost(self._tx_builder.len_neon_tx)

    @staticmethod
    def _holder_tx_cost(neon_tx_len: int) -> int:
        # TODO: should be moved to neon-core-api
        holder_msg_size = 950
        return ((neon_tx_len // holder_msg_size) + 1) * 5000

    def _execution_cost(self) -> int:
        return self._emulator_result.used_gas

    def _iterative_overhead_cost(self) -> int:
        if self._config.cu_priority_fee == 0:
            return 0
        return 0

        # Can be uncommented when Neon EVM will be ready
        # # Add priority fee to the estimated gas
        # iter_cnt = self._emulator_result.iter_cnt
        #
        # # Each iteration requests 1'400'000 CUs
        # # Priority fee is calculated in micro-LAMPORTs per 1 CU
        # priority_cost = iter_cnt * math.ceil(self._config.cu_priority_fee * 1_400_000 / 1_000_000)
        # return priority_cost

    def _alt_cost(self) -> int:
        """Costs to create->extend->deactivate->close an Address Lookup Table
        """
        # ALT is used by TransactionStepFromAccount, TransactionStepFromAccountNoChainId which have 6 fixed accounts
        acc_cnt = len(self._account_list) + 5
        if acc_cnt > ALTLimit.max_tx_account_cnt:
            return 5000 * 12  # ALT ix: create + ceil(256/30) extend + deactivate + close

        return 0

    def _build_account_list(self):
        self._account_list.clear()
        for account in self._emulator_result.solana_account_list:
            self._account_list.append(SolAccountMeta(SolPubKey.from_string(account['pubkey']), False, True))

    def estimate(self, request: Dict[str, Any], block: SolBlockInfo, solana_overrides: Optional[SolanaOverrides] = None):
        self._get_request_param(request)
        self._execute(block, solana_overrides)
        self._build_account_list()

        execution_cost = self._execution_cost()
        tx_size_cost = self._tx_size_cost()
        iterative_cost = self._iterative_overhead_cost()
        alt_cost = self._alt_cost()

        # Ethereum's wallets don't accept gas limit less than 21000
        gas = max(execution_cost + tx_size_cost + iterative_cost + alt_cost, 25000)

        LOG.debug(
            f'execution_cost: {execution_cost}, '
            f'tx_size_cost: {tx_size_cost}, '
            f'iterative_cost: {iterative_cost},'
            f'alt_cost: {alt_cost}, '
            f'estimated gas: {gas}'
        )

        return gas
