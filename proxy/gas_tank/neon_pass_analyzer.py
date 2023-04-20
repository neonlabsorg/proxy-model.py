import logging

from typing import Dict, Any, List, Tuple, Optional, Union, Set

import base58

from .gas_tank_types import GasTankSolTxAnalyzer

from ..common_neon.address import NeonAddress
from ..common_neon.config import Config
from ..common_neon.constants import ACCOUNT_SEED_VERSION
from ..common_neon.eth_proto import NeonTx
from ..common_neon.utils import NeonTxInfo
from ..common_neon.solana_tx import SolPubKey


LOG = logging.getLogger(__name__)

EVM_LOADER_CREATE_ACCT = 0x28
EVM_LOADER_CALL_FROM_RAW_TRX = 0x1f

CLAIM_TO_METHOD_ID = bytes.fromhex('67d1c218')

TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN_APPROVE = 0x04
TOKEN_INIT_ACCT_2 = 0x10
TOKEN_TRANSFER = 0x03


NPSolTx = Dict[str, Any]
NPSolIx = Dict[str, Any]


class NPTxParser:
    def __init__(self, tx: NPSolTx):
        self.acct_key_list = self._get_acct_key_list(tx)
        self._ix_list: List[NPSolIx] = tx['transaction']['message']['instructions']
        self._inner_ix_dict: List[NPSolIx] = tx['meta']['innerInstructions']

    @staticmethod
    def _get_acct_key_list(tx: NPSolTx) -> List[str]:
        acct_key_list = tx["transaction"]["message"]["accountKeys"]
        lookup_key_list = tx["meta"].get('loadedAddresses', None)
        if lookup_key_list is not None:
            acct_key_list += lookup_key_list['writable'] + lookup_key_list['readonly']
        return acct_key_list

    def _is_req_ix(self, ix: NPSolIx, req_prg_id: str, req_tag_id: int) -> bool:
        prg_id = self.acct_key_list[ix['programIdIndex']]
        return prg_id == req_prg_id and base58.b58decode(ix['data'])[0] == req_tag_id

    def find_ix_list(self, caption: str, prg_id: str, tag_id: int) -> List[Tuple[int, NPSolIx]]:
        ix_list = [(idx, ix) for idx, ix in enumerate(self._ix_list) if self._is_req_ix(ix, prg_id, tag_id)]
        if len(ix_list) == 0:
            LOG.debug(f'instructions for instruction {caption} not found')
        return ix_list

    def find_inner_ix(self, caption: str, ix_idx: int, prg_id: str, tag_id: int) -> Optional[NPSolIx]:
        inner_ix_list = None
        for entry in self._inner_ix_dict:
            if entry['index'] == ix_idx:
                inner_ix_list = entry['instructions']
                break

        if inner_ix_list is None:
            LOG.debug(f'Inner instruction list ({caption}) for instruction {ix_idx} not found')
            return None

        for ix in inner_ix_list:
            if self._is_req_ix(ix, prg_id, tag_id):
                return ix

        LOG.debug(f'Inner instruction {caption} for instruction {ix_idx} not found')
        return None


class NeonPassAnalyzer(GasTankSolTxAnalyzer):
    name = 'NeonPass'

    def __init__(self, config: Config, token_whitelist: Union[bool, Set[str]]):
        self._config = config
        self._evm_loader_id = str(config.evm_loader_id)
        self._token_whitelist = token_whitelist
        if isinstance(self._token_whitelist, bool) and self._token_whitelist:
            self._has_token_whitelist = True
        else:
            self._has_token_whitelist = len(self._token_whitelist) > 0

    def _check_on_neon_pass_tx(self, tx: NPSolTx) -> Optional[Tuple[NeonAddress, NeonTxInfo]]:
        tx_parser = NPTxParser(tx)

        # Finding instructions specific for neon-pass
        # NeonPass generates the sequence:
        # neon.CreateAccount -> token.Approve -> neon.callFromRawEthereumTrx (call claim method of ERC20)
        # Additionally:
        # call instruction internally must:
        #   1. Create token account (token.init_v2)
        #   2. Transfer tokens (token.transfer)
        # First: select all instructions that can form such chains
        create_ix_list = self._find_evm_ix_list(tx_parser, 'create account', EVM_LOADER_CREATE_ACCT)
        if not len(create_ix_list):
            return None

        approve_ix_list = self._find_token_ix_list(tx_parser, 'approve', TOKEN_APPROVE)
        if not len(approve_ix_list):
            return None

        call_ix_list = self._find_evm_ix_list(tx_parser, 'call', EVM_LOADER_CALL_FROM_RAW_TRX)
        if not len(call_ix_list):
            return None

        for _create_idx, create_ix in create_ix_list:
            for _approve_idx, approve_ix in approve_ix_list:
                for _call_idx, call_ix in call_ix_list:
                    if (_create_idx > _approve_idx) or (_approve_idx > _call_idx):
                        LOG.debug('wrong order')
                        continue

                    init_token2_ix = self._find_token_inner_ix(tx_parser, 'init_token2', _call_idx, TOKEN_INIT_ACCT_2)
                    if init_token2_ix is None:
                        continue

                    transfer_ix = self._find_token_inner_ix(tx_parser, 'token_transfer', _call_idx, TOKEN_TRANSFER)
                    if transfer_ix is None:
                        continue

                    if not self._check_init_token2_transfer_ix(init_token2_ix, transfer_ix):
                        continue

                    neon_tx: Optional[NeonTxInfo] = self._get_neon_tx(tx_parser, create_ix, approve_ix, call_ix)
                    if neon_tx is None:
                        continue

                    account = NeonAddress(base58.b58decode(create_ix['data'])[1:][:20])
                    return account, neon_tx

        return None

    def _find_evm_ix_list(self, tx_parser: NPTxParser, caption: str, tag_id: int) -> List[Tuple[int, NPSolIx]]:
        return tx_parser.find_ix_list(caption, self._evm_loader_id, tag_id)

    @staticmethod
    def _find_token_ix_list(tx_parser: NPTxParser, caption: str, tag_id: int) -> List[Tuple[int, NPSolIx]]:
        return tx_parser.find_ix_list(caption, TOKEN_PROGRAM_ID, tag_id)

    @staticmethod
    def _find_token_inner_ix(tx_parser: NPTxParser, caption: str, ix_idx: int, tag_id: int) -> Optional[NPSolIx]:
        return tx_parser.find_inner_ix(caption, ix_idx, TOKEN_PROGRAM_ID, tag_id)

    def _get_neon_tx(self, tx_parser: NPTxParser,
                     create_acct_ix: NPSolIx,
                     approve_ix: NPSolIx,
                     call_ix: NPSolIx) -> Optional[NeonTxInfo]:
        # Must use the same Operator account
        approve_acct_idx = approve_ix['accounts'][2]
        call_acct_idx = call_ix['accounts'][0]
        if approve_acct_idx != call_acct_idx:
            LOG.debug(f"approve_account [{approve_acct_idx}] != call_account [{call_acct_idx}]")
            return None

        data = base58.b58decode(call_ix['data'])
        try:
            neon_tx = NeonTx.from_string(data[5:])
        except (Exception,):
            LOG.debug('bad transaction')
            return None

        erc20 = neon_tx.toAddress
        method_id = neon_tx.callData[:4]
        source_token = neon_tx.callData[4:36]
        target_neon_acct = neon_tx.callData[48:68]

        created_acct = base58.b58decode(create_acct_ix['data'])[1:][:20]
        if created_acct != target_neon_acct:
            LOG.debug(f"Created account {created_acct.hex()} and target {target_neon_acct.hex()} are different")
            return None

        sol_caller, _ = SolPubKey.find_program_address(
            [ACCOUNT_SEED_VERSION, b"AUTH", erc20, bytes(12) + neon_tx.sender],
            self._config.evm_loader_id
        )
        if SolPubKey.from_string(tx_parser.acct_key_list[approve_ix['accounts'][1]]) != sol_caller:
            LOG.debug(f"{tx_parser.acct_key_list[approve_ix['accounts'][1]]} != {sol_caller}")
            return None

        # CreateERC20TokenAccount instruction must use ERC20-wrapper from whitelist
        if not self._is_allowed_contract('0x' + erc20.hex()):
            LOG.debug(f'0x{erc20.hex()} is not whitelisted ERC20 contract')
            return None

        if method_id != CLAIM_TO_METHOD_ID:
            LOG.debug(f'bad method: {method_id}')
            return None

        claim_key = base58.b58decode(tx_parser.acct_key_list[approve_ix['accounts'][0]])
        if claim_key != source_token:
            LOG.debug(f'Claim token account 0x{claim_key.hex()} != approve token account 0x{source_token.hex()}')
            return None

        return NeonTxInfo.from_neon_tx(neon_tx)

    @staticmethod
    def _check_init_token2_transfer_ix(init_token2_ix: Dict[str, Any],
                                       transfer_ix: Dict[str, Any]) -> bool:
        created_acct_idx = init_token2_ix['accounts'][0]
        transfer_target_acct_idx = transfer_ix['accounts'][1]

        if created_acct_idx != transfer_target_acct_idx:
            LOG.debug(f"created_account [{created_acct_idx}] != transfer_account [{transfer_target_acct_idx}]")
            return False

        return True

    def _is_allowed_contract(self, contract_addr: str) -> bool:
        if isinstance(self._token_whitelist, bool) and self._token_whitelist:
            return True

        return contract_addr.lower() in self._token_whitelist

    def process(self, tx: Dict[str, Any]) -> Optional[Tuple[NeonAddress, NeonTxInfo]]:
        if not self._has_token_whitelist:
            return None

        return self._check_on_neon_pass_tx(tx)
