import logging

from typing import List

from ..common_neon.db.constats_db import ConstantsDB
from ..common_neon.db.db_connect import DBConnection
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.solana_tx import SolCommit
from ..common_neon.config import Config
from ..common.logger import Logger

from ..statistic.indexer_service import IndexerStatService

from .indexer import Indexer
from .indexer_db import IndexerDB
from .indexer_base import get_start_slot, get_first_slot


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

        self._db_conn = DBConnection(self._config)
        self._stat_service = IndexerStatService(self._config)

        self._first_slot = 0
        self._start_slot = 0
        self._finalized_slot = 0

    def start(self):
        self._stat_service.start()

        constants_db = ConstantsDB(self._db_conn)
        self._init_slot_range(constants_db)

        reindex_start_slot = self._get_reindex_start_slot()
        db_list = self._get_reindex_db_list(constants_db, reindex_start_slot)

        db = IndexerDB.from_range(self._config, self._db_conn, '')
        indexer = Indexer(self._config, db)
        indexer.run()

    def _init_slot_range(self, constants_db: ConstantsDB) -> None:
        solana = SolInteractor(self._config, self._config.solana_url)

        last_known_slot = constants_db.get('min_receipt_block_slot', 0)

        self._first_slot = get_first_slot(solana)
        self._finalized_slot = solana.get_block_slot(SolCommit.Finalized)
        self._start_slot = get_start_slot(self._config, solana, last_known_slot)

    @staticmethod
    def _get_first_slot(self, solana: SolInteractor) -> int:
        first_slot = solana.get_first_available_block()
        if first_slot > 0:
            first_slot += 512
        return first_slot

    def _get_reindex_db_list(self, constants_db: ConstantsDB, reindex_start_slot: str) -> List[IndexerDB]:
        db_list: List[IndexerDB] = list()
        if not self._config.reindex_start_slot or not self._config.reindex_thread_cnt:
            return db_list

        prev_reindex_start_slot = constants_db.get('reindex_start_slot', self._config.reindex_start_slot)
        max_start_slot = 0

        for key in constants_db.keys():
            if not key.endswith(IndexerDB.base_start_slot_name):
                continue

            prefix = key[:-len(IndexerDB.base_start_slot_name + 1)]
            db = IndexerDB.from_db(self._config, self._db_conn, prefix)
            if reindex_start_slot == prev_reindex_start_slot:
                db_list.append(db)
                max_start_slot = max(max_start_slot, db.start_slot)
            else:
                db.done()

        return db_list

    def _get_reindex_start_slot(self) -> str:
        reindex_start_slot = self._config.reindex_start_slot.upper().strip()
        if reindex_start_slot == 'CONTINUE':
            start_slot = self._config.start_slot.upper().strip()
            if start_slot != 'LATEST':
                LOG.error(f'REINDEX_START_SLOT={reindex_start_slot} is valid only for START_SLOT=LATEST')
                raise ValueError('Wrong value in REINDEX_START_SLOT')
            return reindex_start_slot

        try:
            reindex_start_int_slot = int(reindex_start_slot)
            if reindex_start_int_slot >= self._finalized_slot:
                raise ValueError('Too big')

            return str(reindex_start_int_slot)
        except (Exception,):
            pass

        LOG.error(
            f'Wrong value REINDEX_START_SLOT={reindex_start_slot}, '
            f'valid values: CONTINUE, < {self._finalized_slot}'
        )
        raise ValueError('Wrong value in REINDEX_START_SLOT')
