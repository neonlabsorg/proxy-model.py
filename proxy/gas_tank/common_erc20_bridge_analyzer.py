import logging
from typing import Set, Union

from .gas_tank import GasTankState, GasTankNeonTxAnalyzer, GasTankTxInfo
from ..common_neon.address import NeonAddress

LOG = logging.getLogger(__name__)

# keccak256("Transfer(address,address,uint256)")
TRANSFER_EVENT = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


class CommonERC20BridgeAnalyzer(GasTankNeonTxAnalyzer):
    name = 'CommonERC20'

    # token_whitelist - the white list of tokens, transfers to which lead to gas-less transactions.
    # This set should contain ERC20 addresses separated by comma.
    def __init__(self, token_whitelist: Union[bool, Set[str]]):
        self._token_whitelist = token_whitelist
        if isinstance(self._token_whitelist, bool) and self._token_whitelist:
            self._has_token_whitelist = True
        else:
            self._has_token_whitelist = len(self._token_whitelist) > 0

    def process(self, neon_tx: GasTankTxInfo, state: GasTankState) -> bool:
        if not self._has_token_whitelist:
            return False

        call_data = bytes.fromhex(neon_tx.neon_tx.calldata[2:])
        LOG.debug(f'callData: {call_data.hex()}')

        for event in neon_tx.iter_events():
            if len(event['topics']) != 3:
                continue

            if event['topics'][0] != TRANSFER_EVENT or event['topics'][1] != '0x' + 64*'0':
                continue

            token_id = event['address']
            if token_id not in self._token_whitelist:
                continue

            to = NeonAddress(bytes.fromhex(event['topics'][2][2:])[12:])
            amount = int.from_bytes(bytes.fromhex(event['data'][2:]), 'big')
            LOG.info(f'Common ERC20 bridge transfer: {amount} of {token_id} token to {to}')
            state.allow_gas_less_tx(to)
            return True
        return False
