from typing import List, Type

from ..common_neon.neon_instruction import EvmIxCode

from ..indexer.indexed_objects import NeonAccountInfo
from ..indexer.neon_ix_decoder import (
    DummyIxDecoder,
    TxExecFromDataIxDecoder, TxExecFromAccountIxDecoder,
    TxStepFromDataIxDecoder, TxStepFromAccountIxDecoder, TxStepFromAccountNoChainIdIxDecoder,
    CancelWithHashIxDecoder
)


class OldTxExecFromDataIxDecoderV14(TxExecFromDataIxDecoder):
    _ix_code = EvmIxCode.OldTxExecFromDataV14
    _is_deprecated = True


class OldTxExecFromDataIxDecoderV111(TxExecFromDataIxDecoder):
    _ix_code = EvmIxCode.OldTxExecFromDataV111
    _is_deprecated = True


class OldTxExecFromAccountIxDecoderV14(TxExecFromAccountIxDecoder):
    _ix_code = EvmIxCode.OldTxExecFromAccountV14
    _is_deprecated = True


class OldTxStepFromAccountIxDecoderV14(TxStepFromAccountIxDecoder):
    _ix_code = EvmIxCode.OldTxStepFromAccountV14
    _is_deprecated = True


class OldTxStepFromDataIxDecoderV14(TxStepFromDataIxDecoder):
    _ix_code = EvmIxCode.OldTxStepFromDataV14
    _is_deprecated = True


class OldTxStepFromAccountNoChainIdIxDecoderV14(TxStepFromAccountNoChainIdIxDecoder):
    _ix_code = EvmIxCode.OldTxStepFromAccountNoChainIdV14
    _is_deprecated = True


class OldCancelWithHashIxDecoderV14(CancelWithHashIxDecoder):
    _ix_code = EvmIxCode.OldCancelWithHashV14
    _is_deprecated = True


class OldCreateAccountIxDecoderV14(DummyIxDecoder):
    _ix_code = EvmIxCode.OldCreateAccountV14
    _is_deprecated = True

    def execute(self) -> bool:
        ix = self.state.sol_neon_ix
        if len(ix.ix_data) < 21:
            return self._decoding_skip(f'not enough data to get NeonAccount {len(ix.ix_data)}')

        neon_address = '0x' + ix.ix_data[1:21].hex()
        solana_address = ix.get_account(2)

        account_info = NeonAccountInfo(
            neon_address,
            0,
            solana_address,
            None,
            ix.block_slot,
            ix.sol_sig
        )
        return self._decoding_success(account_info, 'create NeonAccount')


class OldDepositIxDecoderV14(DummyIxDecoder):
    _ix_code = EvmIxCode.OldDepositV14
    _is_deprecated = True

    def execute(self) -> bool:
        return self._decoding_success(None, 'deposit NEONs')


def get_neon_ix_decoder_deprecated_list() -> List[Type[DummyIxDecoder]]:
    ix_decoder_list: List[Type[DummyIxDecoder]] = [
        OldTxExecFromDataIxDecoderV14,
        OldTxExecFromDataIxDecoderV111,
        OldTxExecFromAccountIxDecoderV14,
        OldTxStepFromDataIxDecoderV14,
        OldTxStepFromAccountIxDecoderV14,
        OldTxStepFromAccountNoChainIdIxDecoderV14,
        OldCreateAccountIxDecoderV14,
        OldDepositIxDecoderV14
    ]
    for IxDecoder in ix_decoder_list:
        assert IxDecoder.is_deprecated(), f"{IxDecoder.name()} is NOT deprecated!"

    return ix_decoder_list
