import abc
from typing import Optional, Dict, List

from .executor_mng import MPExecutorMng
from .mempool_api import MPGasPriceTokenRequest, MPGasPriceRequest, MPGasPriceResult
from .mempool_periodic_task import MPPeriodicTaskLoop

from ..common_neon.config import Config
from ..common_neon.constants import ONE_BLOCK_SEC
from ..common_neon.evm_config import EVMConfig
from ..common_neon.solana_tx import SolPubKey


class IGasPriceUser(abc.ABC):
    @abc.abstractmethod
    def on_gas_price(self, gas_price: MPGasPriceResult) -> None:
        pass


class MPGasPriceTaskLoop(MPPeriodicTaskLoop[MPGasPriceRequest, MPGasPriceResult]):
    _default_sleep_sec = ONE_BLOCK_SEC * 16

    def __init__(self, config: Config, executor_mng: MPExecutorMng, user: IGasPriceUser) -> None:
        super().__init__(name="gas-price", sleep_sec=self._default_sleep_sec, executor_mng=executor_mng)
        self._user = user
        self._gas_price: Optional[MPGasPriceResult] = None
        self._min_executable_gas_prices: Dict[str, List[int]] = dict()
        self._min_executable_gas_prices_count: int = int(60 / self._default_sleep_sec * config.mempool_gas_price_window)

    def _submit_request(self) -> None:
        req_id = self._generate_req_id()
        token_dict: Dict[int, SolPubKey] = dict()

        last_update_mapping_sec = 0
        sol_price_acct: Optional[SolPubKey] = None
        if self._gas_price:
            sol_price_acct = self._gas_price.sol_price_account
            last_update_mapping_sec = self._gas_price.last_update_mapping_sec
            token_dict: Dict[int, SolPubKey] = {
                token_price.chain_id: token_price.token_price_account for token_price in self._gas_price.token_list
            }

        evm_cfg = EVMConfig()
        token_list = [
            MPGasPriceTokenRequest(
                chain_id=token_info.chain_id,
                token_name=token_info.token_name,
                price_account=token_dict.get(token_info.chain_id, None),
            )
            for token_info in evm_cfg.token_info_list
        ]

        mp_req = MPGasPriceRequest(
            req_id=req_id,
            last_update_mapping_sec=last_update_mapping_sec,
            sol_price_account=sol_price_acct,
            token_list=token_list,
        )

        self._submit_request_to_executor(mp_req)

    def _process_error(self, _: MPGasPriceRequest) -> None:
        pass

    async def _process_result(self, _: MPGasPriceRequest, mp_res: MPGasPriceResult) -> None:
        for token_list in mp_res.token_list:
            if token_list.chain_id not in self._min_executable_gas_prices:
                self._min_executable_gas_prices[token_list.chain_id] = list()

            print("::::::::::::::::::")
            print(f":::::::::: _process_result() ::: chain_id: {token_list.chain_id}")
            print(f":::::::::: BEFORE :::")
            print(f":::::::::::::::::: min_executable_gas_price: {token_list.min_executable_gas_price}")
            print(f":::::::::::::::::: {[gas_price for gas_price in self._min_executable_gas_prices[token_list.chain_id]]} ::::::::::")

            if token_list.min_executable_gas_price > 0:
                min_executable_gas_prices = self._min_executable_gas_prices[token_list.chain_id]
                min_executable_gas_prices.append(token_list.min_executable_gas_price)

                while self._min_executable_gas_prices_count <= len(min_executable_gas_prices):
                    min_executable_gas_prices.pop(0)

            min_executable_gas_price = min(min_executable_gas_prices)

            print(f":::::::::: AFTER :::")
            print(f":::::::::::::::::: min_executable_gas_price: {min_executable_gas_price}")
            print(f":::::::::::::::::: {[gas_price for gas_price in self._min_executable_gas_prices[token_list.chain_id]]} ::::::::::")
            print("::::::::::::::::::")

            if min_executable_gas_price > 0:
                token_list.up_min_executable_gas_price(min_executable_gas_price)

        self._gas_price = mp_res
        self._user.on_gas_price(mp_res)
