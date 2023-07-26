import logging

from typing import List

from ..common_neon.db.constats_db import ConstantsDB
from ..common_neon.db.db_connect import DBConnection
from ..common_neon.config import Config
from ..common.logger import Logger

from ..statistic.indexer_service import IndexerStatService

from .indexer import Indexer, IndexerDBCtx


LOG = logging.getLogger(__name__)


def _set_global_starting_slot(self, slot: int) -> None:
    if self._starting_slot_name == self._global_starting_slot_name:
        return
    elif self._global_starting_slot_name not in self._constants_db:
        return

    old_slot = self._constants_db[self._global_starting_slot_name]
    if old_slot > slot:
        self._constants_db.set_if_same(self._global_starting_slot_name, old_slot, slot)


class NeonIndexerApp:
    def __init__(self):
        Logger.setup()
        self._config = Config()
        LOG.info(f'Running indexer with params: {self._config.as_dict()}')

        self._db = DBConnection(self._config)
        self._stat_service = IndexerStatService(self._config)

    def start(self):
        self._stat_service.start()

        ctx = IndexerDBCtx.from_range(self._config, self._db, '')

        indexer = Indexer(ctx)
        indexer.run()

    def _get_ctx_list(self) -> List[IndexerDBCtx]:
        ctx_list: List[IndexerDBCtx] = list()

        constants_db = ConstantsDB(self._db)
        for key in constants_db.keys():
            if key.endswith(IndexerDBCtx.base_start_slot_name):
                prefix = key[:-len(IndexerDBCtx.base_start_slot_name + 1)]
                ctx = IndexerDBCtx.from_db(self._config, self._db, prefix)
                ctx_list.append(ctx)

        return ctx_list
