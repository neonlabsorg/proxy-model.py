from __future__ import annotations

import logging
from typing import Optional

from ..common_neon.db.db_connect import DBConnection
from ..common_neon.db.constats_db import ConstantsDB
from ..common_neon.solana_tx import SolCommit
from ..common_neon.config import Config
from ..common_neon.solana_interactor import SolInteractor


LOG = logging.getLogger(__name__)


class IndexerDBCtx:
    base_start_slot_name = 'start_block_slot'
    base_stop_slot_name = 'stop_block_slot'

    base_min_used_slot_name = 'min_receipt_block_slot'

    def __init__(self, config: Config, db: DBConnection, prefix: str):
        if len(prefix):
            self._prefix = prefix + '-'
        else:
            self._prefix = ''

        self._config = config
        self._db = db
        self._constants_db = ConstantsDB(db)

        self._start_slot = 0
        self._stop_slot: Optional[int] = None

        self._min_used_slot = 0

        self._start_slot_name = self._prefix + self.base_start_slot_name
        self._stop_slot_name = self._prefix + self.base_stop_slot_name
        self._min_used_slot_name = self._prefix + self.base_min_used_slot_name

    @staticmethod
    def from_db(config: Config, db: DBConnection, prefix: str) -> IndexerDBCtx:
        ctx = IndexerDBCtx(config, db, prefix)

        ctx._start_slot = ctx._constants_db.get(ctx._start_slot_name, 0)
        ctx._stop_slot = ctx._constants_db.get(ctx._stop_slot_name, 0)
        ctx._min_used_slot = ctx._constants_db.get(ctx._min_used_slot_name, 0)

        return ctx

    @staticmethod
    def from_range(config: Config, db: DBConnection, prefix: str,
                   start_slot: int, stop_slot: Optional[int]) -> IndexerDBCtx:
        ctx = IndexerDBCtx(config, db, prefix)

        ctx._start_slot = start_slot
        ctx._min_used_slot = start_slot
        ctx._stop_slot = stop_slot

        ctx._min_used_slot = ctx._constants_db[ctx._min_used_slot_name] = start_slot

        if ctx.is_reindexing_mode():
            ctx._constants_db[ctx._start_slot_name] = start_slot
            ctx._constants_db[ctx._stop_slot_name] = stop_slot

        return ctx

    @property
    def prefix(self) -> str:
        return self._prefix

    @property
    def config(self) -> Config:
        return self._config

    @property
    def db(self) -> DBConnection:
        return self._db

    @property
    def constants_db(self) -> ConstantsDB:
        return self._constants_db

    @property
    def start_slot(self) -> int:
        return self._start_slot

    @property
    def stop_slot(self) -> Optional[int]:
        return self._stop_slot

    def is_reindexing_mode(self) -> bool:
        return self._stop_slot is not None

    @property
    def min_used_slot(self) -> int:
        return self._min_used_slot

    def set_min_used_slot(self, slot: int) -> None:
        if self._min_used_slot >= slot:
            return

        self._min_used_slot = slot
        self._constants_db[self._min_used_slot_name] = slot

    def set_start_slot(self, slot: int) -> None:
        if self._start_slot >= slot:
            return

        self.set_min_used_slot(slot)

        self._start_slot = slot
        if self.is_reindexing_mode():
            self._constants_db[self._start_slot_name] = slot

    def done(self) -> None:
        for k in [self._start_slot_name, self._stop_slot_name, self._min_used_slot_name]:
            if k in self._constants_db:
                del self._constants_db[k]


def get_start_slot(config: Config, solana: SolInteractor, last_known_slot: int) -> int:
    latest_slot = solana.get_block_slot(SolCommit.Finalized)
    start_slot = _get_start_slot_from_config(config, last_known_slot, latest_slot)

    first_slot = solana.get_first_available_block()
    start_slot = max(start_slot, first_slot + 512)
    LOG.info(f'FIRST_AVAILABLE_SLOT={first_slot}: started the receipt slot from {start_slot}')
    return start_slot


def _get_start_slot_from_config(config: Config, last_known_slot: int, latest_slot: int) -> int:
    """
    This function allow to skip some part of history.
    - LATEST - start from the last block slot from Solana
    - CONTINUE - continue from the last parsed slot of from latest
    - NUMBER - first start from the number, then continue from last parsed slot
    """
    last_known_slot = 0 if not isinstance(last_known_slot, int) else last_known_slot
    start_int_slot = 0

    start_slot = config.start_slot
    LOG.info(f'Starting the receipt slot with LAST_KNOWN_SLOT={last_known_slot} and START_SLOT={start_slot}')

    if start_slot not in {'CONTINUE', 'LATEST'}:
        try:
            start_int_slot = min(int(start_slot), latest_slot)
        except (Exception,):
            LOG.error(f'Wrong value START_SLOT={start_slot}: use START_SLOT=0')
            start_int_slot = 0

    if start_slot == 'CONTINUE':
        if last_known_slot > 0:
            LOG.info(f'START_SLOT={start_slot}: started the receipt slot from previous run {last_known_slot}')
            return last_known_slot
        else:
            LOG.info(f'START_SLOT={start_slot}: forced the receipt slot from the latest Solana slot')
            start_slot = 'LATEST'

    if start_slot == 'LATEST':
        LOG.info(f'START_SLOT={start_slot}: started the receipt slot from the latest Solana slot {latest_slot}')
        return latest_slot

    if start_int_slot < last_known_slot:
        LOG.info(f'START_SLOT={start_slot}: started the receipt slot from previous run {last_known_slot}')
        return last_known_slot

    LOG.info(f'START_SLOT={start_slot}: started the receipt slot from {start_int_slot}')
    return start_int_slot
