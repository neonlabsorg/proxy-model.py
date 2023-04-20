import logging

from typing import Set, Union, Optional

from construct import Const, Struct, GreedyBytes, Byte, Bytes, BytesInteger, Int32ub, Int16ub, Int64ub, Switch
from construct import this, Enum

from .gas_tank_types import GasTankNeonTxAnalyzer, GasTankTxInfo

from ..common_neon.address import NeonAddress


LOG = logging.getLogger(__name__)

Signer = Struct(
    "guardianIndex" / Byte,
    "r" / Bytes(32),
    "s" / Bytes(32),
    "v" / Byte,
)

COMPLETE_TRANSFER = bytes.fromhex('c6878519')

VAA = Struct(
    "version" / Const(b"\1"),
    "guardiansetIndex" / Int32ub,
    "signersLen" / Byte,
    "signers" / Signer[this.signersLen],
    "timestamp" / Int32ub,
    "nonce" / Int32ub,
    "emitterChainId" / Int16ub,
    "emitterAddress" / Bytes(32),
    "sequence" / Int64ub,
    "consistencyLevel" / Byte,
    "payloadID" / Enum(Byte, Transfer=1, TransferWithPayload=3),
    "payload" / Switch(this.payloadID, {
            "Transfer": Struct(
                "amount" / BytesInteger(32),
                "tokenAddress" / Bytes(32),
                "tokenChain" / Int16ub,
                "to" / Bytes(32),
                "toChain" / Int16ub,
                "fee" / BytesInteger(32),
            ),
            "TransferWithPayload": Struct(
                "amount" / BytesInteger(32),
                "tokenAddress" / Bytes(32),
                "tokenChain" / Int16ub,
                "to" / Bytes(32),
                "toChain" / Int16ub,
                "fromaddress" / Bytes(32),
                "payload" / GreedyBytes,
            ),
        },
        default=GreedyBytes,
    )
)


class PortalTxAnalyzer(GasTankNeonTxAnalyzer):
    name = 'Portal'
    # token_whitelist - the whitelist of tokens for the transfer of which to provide gas-less transactions
    #    this set should contain next items: "tokenChain:tokenAddress",
    #    where `tokenChain` is originally chain of token in terms of Portal bridge numbers
    #          `tokenAddress` is address of token in hexadecimal lowercase form with '0x' prefix

    def __init__(self, token_whitelist: Union[bool, Set[str]]):
        self._token_whitelist = token_whitelist
        if isinstance(self._token_whitelist, bool) and self._token_whitelist:
            self._has_token_whitelist = True
        else:
            self._has_token_whitelist = len(self._token_whitelist) > 0

    def process(self, neon_tx: GasTankTxInfo) -> Optional[NeonAddress]:
        if not self._has_token_whitelist:
            return None

        call_data = bytes.fromhex(neon_tx.neon_tx.calldata[2:])
        LOG.debug(f'callData: {call_data.hex()}')
        if call_data[0:4] != COMPLETE_TRANSFER:
            return None

        offset = int.from_bytes(call_data[4:36], 'big')
        length = int.from_bytes(call_data[36:68], 'big')
        data = call_data[36+offset:36+offset+length]
        vaa = VAA.parse(data)

        token_address = NeonAddress(vaa.payload.tokenAddress[12:32])
        token_id = f"{vaa.payload.tokenChain}:{token_address}"
        if isinstance(self._token_whitelist, bool):
            pass
        elif token_id not in self._token_whitelist:
            return None

        to = NeonAddress(vaa.payload.to[12:])
        LOG.info(f"Portal transfer: {vaa.payload.amount} of {token_id} token to {to}")
        return to
