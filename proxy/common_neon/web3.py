import dataclasses

from web3 import Web3
from web3.module import Module
from web3.method import Method, default_root_munger
from web3.providers.base import BaseProvider
from typing import Optional, Tuple, Callable, Union, Dict, List, Any
from web3.types import RPCEndpoint, TxParams, HexBytes, ChecksumAddress, Address, BlockIdentifier, LatestBlockParam
from ..common_neon.solana_tx import SolAccountData, SolPubKey
from ..common_neon.data import SolanaOverrides


@dataclasses.dataclass
class NeonAccountData:
    status: str
    address: HexBytes
    transactionCount: int
    balance: int
    chainId: int
    solanaAddress: str
    contractSolanaAddress: str

@dataclasses.dataclass
class EmulateParams:
    tag: Optional[BlockIdentifier]
    solana_overrides: Optional[SolanaOverrides]

    def __init__(self, tag: Optional[BlockIdentifier] = None, solana_overrides: Optional[SolanaOverrides] = None):
        self.tag = tag
        self.solana_overrides = solana_overrides

    def to_dict(self) -> Dict[str, Any]:
        result = dict()
        if self.tag is not None:
            result['tag'] = self.tag
        if self.solana_overrides is not None:
            result['solana_overrides'] = {
                k.__str__(): None if v is None else {
                    'lamports': v.lamports,
                    'data': v.data.hex(),
                    'owner': v.owner.__str__(),
                    'executable': v.executable,
                    'rent_epoch': v.rent_epoch,
                }
                for k, v in self.solana_overrides.items()
            }
        return result

class Neon(Module):
    _neon_emulate = RPCEndpoint('neon_emulate')

    def _neon_emulate_munger(self, tx: bytearray, params: Optional[EmulateParams]) -> Tuple[str, Optional[Dict[str,Any]]]:
        if params is None:
            return (bytes(tx).hex(),)
        else:
            return (bytes(tx).hex(), params.to_dict())

    neon_emulate: Method[
        Callable[[bytearray, Optional[EmulateParams]], Dict[str, Any]]
    ] = Method(
        _neon_emulate,
        mungers=[_neon_emulate_munger],
    )

    _neon_estimateGas = RPCEndpoint('neon_estimateGas')

    def _neon_estimateGas_munger(
        self, transaction: TxParams, params: Optional[EmulateParams]
    ) -> Tuple[TxParams, Optional[Dict[str,Any]]]:
        return transaction, params.to_dict()
        
    neon_estimateGas: Method[
        Callable[[TxParams, Optional[EmulateParams]], int]
    ] = Method(
        _neon_estimateGas,
        mungers=[_neon_estimateGas_munger],
    )

    _neon_getEvmParams = RPCEndpoint('neon_getEvmParams')

    neon_getEvmParams = Method(
        _neon_getEvmParams,
        mungers=[],
    )

    _neon_gasPrice = RPCEndpoint('neon_gasPrice')

    neon_gasPrice = Method(
        _neon_gasPrice,
        mungers=[default_root_munger]
    )

    _neon_getAccount = RPCEndpoint('neon_getAccount')

    def _get_account_munger(
        self,
        account: Union[Address, ChecksumAddress, str],
        block_identifier: Optional[BlockIdentifier] = None,
    ) -> Tuple[str, BlockIdentifier]:
        if block_identifier is None:
            block_identifier = 'latest'
        if isinstance(account, bytes):
            account = '0x' + account.hex()
        return account, block_identifier

    _neon_get_account: Method[
        Callable[
            [Union[Address, ChecksumAddress], Optional[BlockIdentifier]],
            NeonAccountData
        ]
    ] = Method(
        _neon_getAccount,
        mungers=[_get_account_munger],
    )

    def get_neon_account(
        self,
        account: Union[Address, ChecksumAddress, str],
        block_identifier: Optional[BlockIdentifier] = None,
    ) -> NeonAccountData:
        result = self._neon_get_account(account, block_identifier)

        def _to_int(_s) -> int:
            if isinstance(_s, str):
                return int(_s, 16)
            return _s
        return NeonAccountData(
            status=result.status,
            address=result.address,
            solanaAddress=result.solanaAddress,
            contractSolanaAddress=result.contractSolanaAddress,
            chainId=_to_int(result.chainId),
            transactionCount=_to_int(result.transactionCount),
            balance=_to_int(result.balance)
        )


class NeonWeb3(Web3):
    neon: Neon

    def __init__(self, provider:  Optional[BaseProvider] = None):
        super().__init__(provider)
        setattr(self, "neon", Neon(self))
