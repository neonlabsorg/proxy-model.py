from __future__ import annotations

import statistics
import logging

from typing import Dict, Union, List

from .config import Config


LOG = logging.getLogger(__name__)


class MetricsLogger:
    def __init__(self, config: Config):
        self._config = config
        self._counter: int = 0

    def _reset(self):
        self._counter = 0

    def is_print_time(self) -> bool:
        self._counter += 1
        return (self._counter % self._config.metrics_log_skip_cnt) == 0

    def print(self, latest_value_dict: Dict[str, int]):
        msg = ''
        for key, value in latest_value_dict.items():
            msg += f' {key}: {value};'

        LOG.debug(msg)
        self._reset()
