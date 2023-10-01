from __future__ import annotations

import logging
from typing import Optional, Dict

from singleton_decorator import singleton

from .solana_tx import SolPubKey
from .config import Config

from ..neon_core_api.neon_cli import NeonCli


LOG = logging.getLogger(__name__)


@singleton
class ElfParams:
    def __init__(self):
        self._elf_param_dict: Dict[str, any] = {}

    @property
    def treasury_pool_cnt(self) -> int:
        return int(self._elf_param_dict.get('NEON_TREASURY_POOL_COUNT'))

    @property
    def treasury_pool_seed(self) -> bytes:
        return bytes(self._elf_param_dict.get('NEON_TREASURY_POOL_SEED'), 'utf8')

    @property
    def neon_evm_steps(self) -> int:
        return int(self._elf_param_dict.get("NEON_EVM_STEPS_MIN"))

    @property
    def neon_token_mint(self) -> SolPubKey:
        return SolPubKey.from_string(self._elf_param_dict.get("NEON_TOKEN_MINT"))

    @property
    def chain_id(self) -> int:
        return int(self._elf_param_dict.get('NEON_CHAIN_ID'))

    @property
    def neon_evm_version(self) -> Optional[str]:
        return self._elf_param_dict.get("NEON_PKG_VERSION")

    @property
    def neon_evm_revision(self) -> Optional[str]:
        return self._elf_param_dict.get('NEON_REVISION')

    @property
    def neon_gas_limit_multiplier_no_chainid(self) -> int:
        return int(self._elf_param_dict.get('NEON_GAS_LIMIT_MULTIPLIER_NO_CHAINID'))

    def has_params(self) -> bool:
        return len(self._elf_param_dict) > 0

    def is_evm_compatible(self, proxy_version: str) -> bool:
        evm_version = None
        try:
            evm_version = self.neon_evm_version
            major_evm_version, minor_evm_version, _ = evm_version.split('.')
            major_proxy_version, minor_proxy_version, _ = proxy_version.split('.')

            if major_evm_version != major_proxy_version:
                return False
            if minor_evm_version == minor_proxy_version:
                return True

            # Custom version compatibility checking
            if (major_proxy_version == '1') and (minor_proxy_version == '4') and (minor_evm_version in ('2', '4')):
                return True
            #

            return False
        except BaseException as exc:
            LOG.error(f'Cannot compare evm version {evm_version} with proxy version {proxy_version}.', exc_info=exc)
            return False

    @property
    def elf_param_dict(self) -> Dict[str: str]:
        return self._elf_param_dict

    def read_elf_param_dict_from_net(self, config: Config) -> ElfParams:
        if not self.has_params():
            LOG.debug("Read ELF params")
        elf_param_dict: Dict[str, str] = {}
        params = NeonCli(config, False).call("neon-elf-params")
        for param in params:
            if param.startswith('NEON_'):
                elf_param_dict.setdefault(param, params[param])

        for param, value in elf_param_dict.items():
            if self._elf_param_dict.get(param) != value:
                LOG.debug(f"new ELF param: {param}: {value}")
        self.set_elf_param_dict(elf_param_dict)
        return self

    def set_elf_param_dict(self, elf_param_dict: Dict[str, str]) -> ElfParams:
        self._elf_param_dict = elf_param_dict
        return self
