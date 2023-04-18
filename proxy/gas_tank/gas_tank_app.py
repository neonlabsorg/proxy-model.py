import os
import logging
from typing import Dict, List, Tuple

from ..common_neon.address import NeonAddress
from ..common_neon.config import Config
from ..common.logger import Logger

from .gas_tank import GasTank, GasTankNeonTxAnalyzer, GasTankSolTxAnalyzer
from .portal_analyzer import PortalTxAnalyzer
from .common_erc20_bridge_analyzer import CommonERC20BridgeAnalyzer
from .neon_pass_analyzer import NeonPassAnalyzer


LOG = logging.getLogger(__name__)


class GasTankApp:
    def __init__(self):
        Logger.setup()
        LOG.info("GasTank application is starting ...")
        config = Config()
        faucet_url = os.environ['FAUCET_URL']
        sol_tx_analyzer_list = self._get_neon_pass_contract_cfg(config)

        max_conf = float(os.environ.get('MAX_CONFIDENCE_INTERVAL', 0.02))

        LOG.info(f"""
            Construct GasTank with params: {str(config)}
            faucet_url: {faucet_url},
            Max confidence interval: {max_conf}
        """)

        neon_tx_analyzer_list = self._get_portal_bridge_contract_cfg() + self._get_common_erc20_bridge_contract_cfg()
        neon_tx_analyzer_dict: Dict[NeonAddress, GasTankNeonTxAnalyzer] = dict()
        for neon_address, tx_analyzer in neon_tx_analyzer_list:
            if neon_address in neon_tx_analyzer_dict:
                raise RuntimeError(f'Address {neon_address} already specified to analyze')
            neon_tx_analyzer_dict[neon_address] = tx_analyzer

        self._gas_tank = GasTank(config, sol_tx_analyzer_list, neon_tx_analyzer_dict, faucet_url, max_conf)

    @staticmethod
    def _get_neon_pass_contract_cfg(config: Config) -> List[GasTankSolTxAnalyzer]:
        neon_pass_whitelist = os.environ.get('INDEXER_ERC20_WRAPPER_WHITELIST', '')
        if len(neon_pass_whitelist) == 0:
            return list()

        if neon_pass_whitelist == 'ANY':
            neon_pass_whitelist = True
        else:
            neon_pass_whitelist = set(s.lower() for s in neon_pass_whitelist.split(','))

        return [NeonPassAnalyzer(config, neon_pass_whitelist)]

    @staticmethod
    def _get_portal_bridge_contract_cfg() -> List[Tuple[NeonAddress, GasTankNeonTxAnalyzer]]:
        portal_bridge_contract_list = os.environ.get('PORTAL_BRIDGE_CONTRACTS', None)
        portal_bridge_token_whitelist = os.environ.get('PORTAL_BRIDGE_TOKENS_WHITELIST', None)
        if (portal_bridge_contract_list is None) != (portal_bridge_token_whitelist is None):
            raise RuntimeError(
                'Need to specify both PORTAL_BRIDGE_CONTRACTS & PORTAL_BRIDGE_TOKENS_WHITELIST environment variables'
            )

        elif portal_bridge_contract_list is None:
            return list()

        if portal_bridge_token_whitelist == 'ANY':
            token_whitelist = True
        else:
            token_whitelist = set(portal_bridge_token_whitelist.split(','))

        portal_analyzer = PortalTxAnalyzer(token_whitelist)
        return [(NeonAddress(address), portal_analyzer) for address in portal_bridge_contract_list.split(',')]

    @staticmethod
    def _get_common_erc20_bridge_contract_cfg() -> List[Tuple[NeonAddress, GasTankNeonTxAnalyzer]]:
        erc20_bridge_contract_list = os.environ.get('ERC20_BRIDGE_CONTRACTS', None)
        erc20_bridge_token_whitelist = os.environ.get('ERC20_BRIDGE_TOKENS_WHITELIST', None)
        if (erc20_bridge_contract_list is None) != (erc20_bridge_token_whitelist is None):
            raise RuntimeError(
                "Need to specify both ERC20_BRIDGE_CONTRACTS & ERC20_BRIDGE_TOKENS_WHITELIST environment variables"
            )
        elif erc20_bridge_contract_list is None:
            return list()

        if erc20_bridge_token_whitelist == 'ANY':
            token_whitelist = True
        else:
            token_whitelist = set(erc20_bridge_token_whitelist.split(','))

        erc20_bridge_analyzer = CommonERC20BridgeAnalyzer(token_whitelist)
        return [(NeonAddress(address), erc20_bridge_analyzer) for address in erc20_bridge_token_whitelist.split(',')]

    def run(self) -> int:
        try:
            self._gas_tank.run()
        except BaseException as exc:
            LOG.error('Failed to start GasTank', exc_info=exc)
            return 1
        return 0
