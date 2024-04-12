from typing import List, Optional

from .executor_mng import MPExecutorMng
from .mempool_api import MPGasPriceRequest, MPGasPriceResult
from .mempool_periodic_task import MPPeriodicTaskLoop

from ..common_neon.config import Config
from ..common_neon.constants import ONE_BLOCK_SEC


class MPGasPriceTaskLoop(MPPeriodicTaskLoop[MPGasPriceRequest, MPGasPriceResult]):
    _default_sleep_sec = ONE_BLOCK_SEC * 16

    def __init__(self, config: Config, executor_mng: MPExecutorMng) -> None:
        super().__init__(name='gas-price', sleep_sec=self._default_sleep_sec, executor_mng=executor_mng)
        self._gas_price: Optional[MPGasPriceResult] = None
        self._min_executable_gas_prices: List[int] = list()
        self._min_executable_gas_prices_count: int = int(
            60 / self._default_sleep_sec * config.mempool_gas_price_window)

    @property
    def gas_price(self) -> Optional[MPGasPriceResult]:
        return self._gas_price

    def _submit_request(self) -> None:
        req_id = self._generate_req_id()
        if self._gas_price is None:
            mp_req = MPGasPriceRequest(req_id=req_id)
        else:
            mp_req = MPGasPriceRequest(
                req_id=req_id,
                last_update_mapping_sec=self._gas_price.last_update_mapping_sec,
                sol_price_account=self._gas_price.sol_price_account,
                neon_price_account=self._gas_price.neon_price_account
            )
        self._submit_request_to_executor(mp_req)

    def _process_error(self, _: MPGasPriceRequest) -> None:
        pass

    async def _process_result(self, _: MPGasPriceRequest, mp_res: MPGasPriceResult) -> None:
        if mp_res.min_executable_gas_price > 0:
            self._min_executable_gas_prices.append(mp_res.min_executable_gas_price)

            while self._min_executable_gas_prices_count <= len(self._min_executable_gas_prices):
                self._min_executable_gas_prices.pop(0)

            min_executable_gas_price = min(self._min_executable_gas_prices)

            if min_executable_gas_price > 0:
                mp_res.up_min_executable_gas_price(min_executable_gas_price)

        self._gas_price = mp_res
