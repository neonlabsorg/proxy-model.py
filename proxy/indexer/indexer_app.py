import logging
import time

from typing import Tuple, List, Optional
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
from .indexer_utils import get_config_start_slot


LOG = logging.getLogger(__name__)


class NeonIndexerApp:
    def __init__(self):
        self._cfg = Config()

        self._db_conn: Optional[DBConnection] = None
        self._stat_service: Optional[IndexerStatService] = None

        self._first_slot = 0
        self._last_known_slot = 0
        self._start_slot = 0
        self._finalized_slot = 0

        self._reindex_ident = ''
        self._reindex_start_slot: Optional[int] = None

    def start(self):
        Logger.setup()
        LOG.info(f'Running indexer with params: {self._cfg.as_dict()}')

        self._stat_service = IndexerStatService(self._cfg)
        self._stat_service.start()

        self._db_conn = DBConnection(self._cfg)
        constants_db = ConstantsDB(self._db_conn)

        self._drop_not_finalized_history()
        self._init_slot_range(constants_db)

        self._start_reindexing(constants_db)
        self._start_indexing()

    def _drop_not_finalized_history(self) -> None:
        db = IndexerDB.from_db(self._cfg, self._db_conn)
        db.drop_not_finalized_history()

    def _init_slot_range(self, constants_db: ConstantsDB) -> None:
        solana = SolInteractor(self._cfg, self._cfg.solana_url)

        self._last_known_slot = last_known_slot = constants_db.get(IndexerDB.base_min_used_slot_name, 0)
        self._first_slot = first_slot = solana.get_first_available_slot()
        self._finalized_slot = finalized_slot = solana.get_finalized_slot()
        self._start_slot = get_config_start_slot(self._cfg, first_slot, finalized_slot, last_known_slot)

    def _start_indexing(self) -> None:
        time.sleep(10)
        db = IndexerDB.from_range(self._cfg, self._db_conn, self._start_slot)
        indexer = Indexer(self._cfg, db)
        indexer.run()

    def _start_reindexing(self, constants_db: ConstantsDB) -> None:
        self._reindex_start_slot, self._reindex_ident = self._get_reindex_start_slot()

        if (self._reindex_start_slot is None) or (not self._cfg.reindex_thread_cnt):
            LOG.info(
                'Skip reindexing: '
                f'{self._cfg.reindex_start_slot_name}={self._reindex_ident}, '
                f'{self._cfg.reindex_thread_cnt_name}={self._cfg.reindex_thread_cnt}'
            )
            return

        db_list = self._load_exist_reindex_ranges(constants_db)
        if self._is_reindex_completed(constants_db, db_list):
            return

        self._add_new_reindex_ranges(db_list)
        self._launch_reindex_threads(db_list)

    def _is_reindex_completed(self, constants_db: ConstantsDB, db_list: List[IndexerDB]) -> bool:
        if self._reindex_ident == self._cfg.continue_slot_name:
            return False

        reindex_ident_name = 'reindex_ident'

        last_reindex_ident = constants_db.get(reindex_ident_name, '<NULL>')
        if (last_reindex_ident == self._reindex_ident) and (not len(db_list)):
            LOG.info(f'Reindexing {self._cfg.reindex_start_slot_name}={self._reindex_ident} was completed...')
            return True

        constants_db[reindex_ident_name] = self._reindex_ident
        return False

    def _load_exist_reindex_ranges(self, constants_db: ConstantsDB) -> List[IndexerDB]:
        db_list: List[IndexerDB] = list()

        for key in constants_db.keys():
            if not key.endswith(IndexerDB.base_start_slot_name):
                continue

            # For example: CONTINUE:213456789-starting_block_slot
            reindex_ident = key[:-len(IndexerDB.base_start_slot_name + 1)]
            start_slot_pos = reindex_ident.find(':')
            db = IndexerDB.from_db(self._cfg, DBConnection(self._cfg), reindex_ident)

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
            else:
                LOG.info(f'Load REINDEX {reindex_ident}')
                db_list.append(db)

        return db_list

    def _add_new_reindex_ranges(self, db_list: List[IndexerDB]) -> None:
        new_db_list = self._build_new_reindex_ranges(db_list)
        new_db_list = self._try_extend_last_db(db_list, new_db_list)
        db_list.extend(new_db_list)

    def _build_new_reindex_ranges(self, db_list: List[IndexerDB]) -> List[IndexerDB]:
        """Reindex slots between the reindexing start slot and indexing start slot.
        Check that the number of ranges is not exceeded.
        """
        total_len = self._start_slot - self._reindex_start_slot + 1
        avail_cnt = max(1, self._cfg.reindex_max_range_cnt - len(db_list))
        need_cnt = int(total_len / self._cfg.reindex_range_len) + 1
        avail_cnt = min(avail_cnt, need_cnt)
        range_len = int(total_len / avail_cnt) + 1

        new_db_list: List[IndexerDB] = list()
        start_slot = self._reindex_start_slot
        while start_slot < self._start_slot:
            # For example: CONTINUE:213456789
            ident = ':'.join([self._reindex_ident, str(start_slot)])
            stop_slot = min(start_slot + range_len, self._start_slot)
            db = IndexerDB.from_range(self._cfg, DBConnection(self._cfg), start_slot, ident, stop_slot)
            new_db_list.append(db)
            start_slot = stop_slot

        return new_db_list

    def _try_extend_last_db(self, db_list: List[IndexerDB], new_db_list: List[IndexerDB]) -> List[IndexerDB]:
        """If it is the fast restart, the number of blocks between restarts is small.
        So here we are trying to merge the last previous range with the new first range.
        """
        if (not len(db_list)) or (not len(new_db_list)):
            return new_db_list

        first_new_db = min(new_db_list, key=lambda x: x.start_slot)
        last_old_db = max(db_list, key=lambda x: x.start_slot)
        if first_new_db.start_slot - last_old_db.stop_slot > self._cfg.reindex_range_len:
            return new_db_list

        last_old_db.set_stop_slot(first_new_db.stop_slot)
        return new_db_list[1:]

    def _get_reindex_start_slot(self) -> Tuple[Optional[int], str]:
        """ Valid variants:
        REINDEXER_START_SLOT=CONTINUE, START_SLOT=LATEST
        REINDEXER_START_SLOT=10123456, START_SLOT=CONTINUE
        REINDEXER_START_SLOT=10123456, START_SLOT=LATEST
        REINDEXER_START_SLOT=10123456, START_SLOT=100
        """
        reindex_ident = self._cfg.reindex_start_slot
        if not len(reindex_ident):
            return None, ''

        if reindex_ident == self._cfg.continue_slot_name:
            if self._cfg.start_slot != self._cfg.latest_slot_name:
                LOG.error(
                    f'Wrong value {self._cfg.reindex_start_slot_name}={self._cfg.continue_slot_name}, '
                    f'it is valid only for {self._cfg.start_slot_name}={self._cfg.latest_slot_name}: '
                    f'forced to disable {self._cfg.reindex_start_slot_name}'
                )
                return None, ''

            LOG.info(
                f'{self._cfg.reindex_start_slot_name}={self._cfg.continue_slot_name}: '
                f'started reindexing from the slot: {self._last_known_slot}'
            )
            return self._last_known_slot, reindex_ident

        try:
            reindex_int_slot = int(reindex_ident)
            if reindex_int_slot >= self._finalized_slot:
                raise ValueError('Too big value')

            LOG.info(
                f'{self._cfg.reindex_start_slot_name}={reindex_ident}: '
                f'started reindexing from the slot: {reindex_int_slot}'
            )

            return reindex_int_slot, reindex_ident

        except (Exception,):
            LOG.error(
                f'Wrong value {self._cfg.reindex_start_slot_name}={reindex_ident}, '
                f'valid values are {self._cfg.latest_slot_name} or an INTEGER less than {self._finalized_slot},'
                f'forced to disable {self._cfg.reindex_start_slot_name}'
            )

        return None, ''

    def _launch_reindex_threads(self, db_list: List[IndexerDB]) -> None:
        """Split the DB list so that the first starting slots are reindexed first.

        For example:
        self._cfg.reindex_thread_cnt = 2
        I -> IndexerDB
        S -> start_slot
        I(S=1) -> IndexerDB(start_slot=1)

        [I(S=1000), I(S=10), I(S=11), I(S=12), I(S=102)]

        ReIndexer(0): [I(S=10), I(S=12),  I(S=1000)]
        ReIndexer(1): [I(S=11), I(S=102)]
        """
        db_list = sorted(db_list, key=lambda x: x.start_slot, reverse=True)
        reindex_db_list_list: List[List[IndexerDB]] = [list() for _ in range(self._cfg.reindex_thread_cnt)]

        idx = 0
        while len(db_list) > 0:
            db = db_list.pop()
            reindex_db_list_list[idx].append(db)
            idx += 1
            if idx >= self._cfg.reindex_thread_cnt:
                idx = 0

        for idx in range(self._cfg.reindex_thread_cnt):
            reindex_db_list = reindex_db_list_list[idx]
            if not len(reindex_db_list):
                break

            reindexer = ReIndexer(idx, self._cfg, reindex_db_list)
            reindexer.start()


class ReIndexer:
    def __init__(self, idx: int, cfg: Config, db_list: List[IndexerDB]):
        self._idx = idx
        self._cfg = cfg
        self._db_list = db_list

    def start(self) -> None:
        """Python has GIL... It can be resolved with separate processes"""
        process = Process(target=self._run)
        process.start()

    def _run(self) -> None:
        """Under the hood it runs the Indexer but in a limited range of slots."""
        LOG.debug(f'Start ReIndexer({self._idx})')
        for db in self._db_list:
            with logging_context(reindex_ident=db.reindex_ident):
                LOG.debug(
                    f'Start to reindex the range {db.start_slot}(->{db.min_used_slot}):{db.stop_slot} '
                    f'on the ReIndexer({self._idx})',
                )
            indexer = Indexer(self._cfg, db)
            indexer.run()
