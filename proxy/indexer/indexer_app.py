import logging

from typing import Dict, Tuple, List, Optional
from multiprocessing import Process

from ..common_neon.db.constats_db import ConstantsDB
from ..common_neon.db.db_connect import DBConnection
from ..common_neon.solana_interactor import SolInteractor
from ..common_neon.config import Config
from ..common.logger import Logger
from ..common_neon.utils.json_logger import logging_context

from ..statistic.indexer_service import IndexerStatService

from .indexer import Indexer
from .indexer_db import IndexerDB
from .indexer_base import get_config_start_slot


LOG = logging.getLogger(__name__)


class NeonIndexerApp:
    def __init__(self):
        Logger.setup()

        self._cfg = Config()
        LOG.info(f'Running indexer with params: {self._cfg.as_dict()}')

        self._db_conn = DBConnection(self._cfg)
        self._constants_db = ConstantsDB(self._db_conn)
        self._stat_service = IndexerStatService(self._cfg)

        self._first_slot = 0
        self._last_known_slot = 0
        self._start_slot = 0
        self._finalized_slot = 0

        self._reindex_ident = ''
        self._reindex_start_slot = 0

    def start(self):
        self._stat_service.start()
        IndexerDB.from_db(self._cfg, self._db_conn).drop_not_finalized_history()

        self._init_slot_range()
        self._start_reindexing()
        self._start_indexing()

    def _init_slot_range(self) -> None:
        solana = SolInteractor(self._cfg, self._cfg.solana_url)

        self._last_known_slot = last_known_slot = self._constants_db.get('min_receipt_block_slot', 0)
        self._first_slot = first_slot = solana.get_first_available_slot()
        self._finalized_slot = finalized_slot = solana.get_finalized_slot()
        self._start_slot = get_config_start_slot(self._cfg, first_slot, finalized_slot, last_known_slot)

    def _start_indexing(self) -> None:
        db = IndexerDB.from_range(self._cfg, self._db_conn, self._start_slot)
        indexer = Indexer(self._cfg, db)
        indexer.run()

    def _start_reindexing(self) -> None:
        self._reindex_ident, self._reindex_start_slot = self._get_reindex_start_slot()

        if (not self._reindex_ident) or (not self._cfg.reindex_thread_cnt):
            LOG.info(
                'Skip reindexing: '
                f'{self._cfg.reindex_start_slot_name}={self._reindex_ident}, '
                f'{self._cfg.reindex_thread_cnt_name}={self._cfg.reindex_thread_cnt}'
            )
            return

        db_dict = self._load_exist_reindex_ranges()
        db_dict = self._add_new_reindex_ranges(db_dict)
        self._launch_reindex_threads(db_dict)

    def _load_exist_reindex_ranges(self) -> Dict[str, IndexerDB]:
        db_dict: Dict[str, IndexerDB] = dict()

        for key in self._constants_db.keys():
            if not key.endswith(IndexerDB.base_start_slot_name):
                continue

            reindex_ident = key[:-len(IndexerDB.base_start_slot_name + 1)]
            start_slot_pos = reindex_ident.find(':')
            db = IndexerDB.from_db(self._cfg, self._db_conn, reindex_ident)

            if start_slot_pos == -1:
                LOG.error(f'Skip wrong REINDEX {reindex_ident}')
                db.done()
            elif self._reindex_ident != reindex_ident[:start_slot_pos]:
                LOG.info(f'Skip old REINDEX {reindex_ident}')
                db.done()
            elif self._first_slot > db.stop_slot:
                LOG.info(
                    f'Skip lost REINDEX {reindex_ident}: '
                    f'first slot ({self._first_slot}) > db.stop_slot {db.stop_slot}'
                )
                db.done()
            elif reindex_ident in db_dict:
                LOG.info(f'Skip duplicate REINDEX {reindex_ident}')
            else:
                LOG.info(f'Load REINDEX {reindex_ident}')
                db_dict[reindex_ident] = db

        return db_dict

    def _add_new_reindex_ranges(self, db_dict: Dict[str, IndexerDB]) -> Dict[str, IndexerDB]:

        return db_dict

    def _get_reindex_start_slot(self) -> Tuple[str, Optional[int]]:
        reindex_ident = self._cfg.reindex_start_slot
        if reindex_ident == self._cfg.continue_slot_name:
            if self._cfg.start_slot != self._cfg.latest_slot_name:
                LOG.error(
                    f'Wrong value {self._cfg.reindex_start_slot_name}={self._cfg.continue_slot_name}, '
                    f'it is valid only for {self._cfg.start_slot_name}={self._cfg.latest_slot_name}: '
                    f'forced to disable {self._cfg.reindex_start_slot_name}'
                )
                return '', None

            LOG.info(
                f'{self._cfg.reindex_start_slot_name}={self._cfg.continue_slot_name}: '
                f'started reindexing from the slot: {self._start_slot}'
            )
            return reindex_ident, self._last_known_slot

        try:
            reindex_int_slot = int(reindex_ident)
            if reindex_int_slot >= self._finalized_slot:
                raise ValueError('Too big value')

            return reindex_ident, reindex_int_slot

        except (Exception,):
            LOG.error(
                f'Wrong value {self._cfg.reindex_start_slot_name}={reindex_ident}, '
                f'valid values are {self._cfg.latest_slot_name} or an INTEGER less than {self._finalized_slot},'
                f'forced to disable {self._cfg.reindex_start_slot_name}'
            )

        return '', None

    def _launch_reindex_threads(self, db_dict: Dict[str, IndexerDB]) -> None:
        db_list = list(db_dict.values())
        cnt = min(1, int(len(db_list) / self._cfg.reindex_thread_cnt) + 1)
        for ident in range(self._cfg.reindex_thread_cnt):
            if not len(db_list):
                break
            reindex_db_list, db_list = db_list[:cnt], db_list[cnt:]
            re_indexer = ReIndexer(ident, self._cfg, reindex_db_list)
            re_indexer.start()


class ReIndexer:
    def __init__(self, ident: int, cfg: Config, db_list: List[IndexerDB]):
        self._ident = ident
        self._cfg = cfg
        self._db_list = db_list
        self._process = Process(target=self._run)

    def start(self) -> None:
        self._process.start()

    def _run(self) -> None:
        LOG.debug(f'Start reIndexer {self._ident}')
        for db in self._db_list:
            with logging_context(reindex_ident=db.reindex_ident):
                LOG.debug(
                    f'Start to reindex the range {db.start_slot}(->{db.min_used_slot}):{db.stop_slot} '
                    f'on the reIndexer {self._ident}',
                )
            indexer = Indexer(self._cfg, db)
            indexer.run()
