import os
import subprocess
import logging
import time
import re

from typing import Dict, Any, List
from multiprocessing import Process

from ..common_neon.config import Config

from .logging_level import NeonCoreApiLoggingLevel


LOG = logging.getLogger(__name__)


class _Service:
    def __init__(self, config: Config, idx: int, solana_url: str):
        self._config = config
        port = config.neon_core_api_port + idx
        self._host = f'127.0.0.1:{port}'
        self._solana_url = solana_url

        # 7-bit C1 ANSI sequences
        self._ansi_escape = re.compile(
            r"""
            \x1B  # ESC
            (?:   # 7-bit C1 Fe (except CSI)
                [@-Z\\-_]
            |     # or [ for CSI, followed by a control sequence
                \[
                [0-?]*  # Parameter bytes
                [ -/]*  # Intermediate bytes
                [@-~]   # Final byte
            )
        """,
            re.VERBOSE,
        )

        self._skip_len = len('2024-02-20T21:59:26.318980Z ')

    def start(self) -> None:
        process = Process(target=self._run)
        process.start()

    def _create_env(self) -> Dict[str, Any]:
        log_level = NeonCoreApiLoggingLevel().level

        env = os.environ.copy()

        env.update(dict(
            RUST_BACKTRACE='1',
            RUST_LOG=log_level,

            SOLANA_URL=self._solana_url,
            NEON_API_LISTENER_ADDR=self._host,
            COMMITMENT='recent',
            NEON_DB_CLICKHOUSE_URLS=';'.join(self._config.ch_dsn_list),
            SOLANA_KEY_FOR_CONFIG=str(self._config.solana_key_for_evm_config),

            # TODO: remove
            KEYPAIR='',
            FEEPAIR=''
        ))

        return env

    def _run(self):
        cmd = ['neon-core-api', '-H', self._host]
        env = self._create_env()

        while True:
            self._run_host_api(cmd, env)
            time.sleep(1)

    def _run_host_api(self, cmd: List[str], env: Dict[str, Any]):
        try:
            LOG.info(f'Start Neon Core API service at the {self._host}')
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                env=env
            )
            while True:
                line = process.stdout.readline()
                if line:
                    if not self._config.debug_core_api:
                        line = self._ansi_escape.sub('', line).replace('"', "'")
                        pos = line.find(' ', self._skip_len) + 1
                        line = line[pos:-1]
                        LOG.debug(line)
                elif process.poll() is not None:
                    break

        except BaseException as exc:
            LOG.warning('Neon Core API finished with error', exc_info=exc)


class NeonCoreApiService:
    def __init__(self, config: Config):
        self._service_list = [_Service(config, idx, url) for idx, url in enumerate(config.solana_url_list)]

    def start(self) -> None:
        for service in self._service_list:
            service.start()
