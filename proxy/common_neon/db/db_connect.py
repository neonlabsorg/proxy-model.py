from __future__ import annotations

import logging
import time
import itertools

from typing import List, Tuple, Any, Optional, Callable

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from .db_config import DBConfig


LOG = logging.getLogger(__name__)


class DBConnection:
    _PGCursor = psycopg2.extensions.cursor
    _PGConnection = psycopg2.extensions.connection

    def __init__(self, cfg: DBConfig):
        self._cfg = cfg
        self._conn: Optional[DBConnection._PGConnection] = None
        self._tx_conn: Optional[DBConnection._PGConnection] = None

    def _connect(self) -> None:
        self._cfg.validate_db_config()

        kwargs = dict(
            dbname=self._cfg.postgres_db,
            user=self._cfg.postgres_user,
            password=self._cfg.postgres_password,
            host=self._cfg.postgres_host,
        )

        if self._cfg.postgres_timeout > 0:
            wait_ms = self._cfg.postgres_timeout * 1000
            kwargs['options'] = (
                f'-c statement_timeout={wait_ms} ' +
                f'-c idle_in_transaction_session_timeout={wait_ms-500} '
            )
            LOG.debug(f'add statement timeout {wait_ms}')

        self._conn = psycopg2.connect(**kwargs)
        self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    def _clear(self) -> None:
        if self._conn is not None:
            self._conn.close()

        self._conn = None
        self._tx_conn = None

    def _cursor(self) -> DBConnection._PGCursor:
        assert self._conn is not None

        if self._tx_conn is not None:
            return self._tx_conn.cursor()
        return self._conn.cursor()

    @property
    def config(self) -> DBConfig:
        return self._cfg

    def is_connected(self) -> bool:

        try:
            if self._conn is None:
                self._connect()

            with self._cursor() as cursor:
                cursor.execute('SELECT 1')

            return True
        except (BaseException, ):
            self._clear()
            return False

    def run_tx(self, action: Callable[[], None]) -> None:
        if self._tx_conn is not None:
            action()
            return

        try:
            for retry in itertools.count():
                try:
                    if self._conn is None:
                        self._connect()

                    with self._conn as tx_conn:
                        self._tx_conn = tx_conn

                        action()
                        self._tx_conn = None
                        return

                except BaseException as exc:
                    self._on_fail_execute(retry, exc)
        finally:
            self._tx_conn = None

    def _on_fail_execute(self, retry: int, exc: BaseException) -> None:
        if isinstance(exc, psycopg2.OperationalError) or isinstance(exc, psycopg2.InterfaceError):
            if retry > 1:
                LOG.debug(f'Fail {retry} on DB connection', exc_info=exc)

            self._clear()
            time.sleep(1)

        else:
            self._clear()
            LOG.error('Unknown fail on DB connection', exc_info=exc)
            raise

    def update_row(self, request: str, value_list: Tuple[Any, ...]) -> None:
        assert self._tx_conn is not None
        with self._tx_conn.cursor() as cursor:
            cursor.execute(request, value_list)

    def update_row_list(self, request: str, row_list: List[List[Any]]):
        assert self._tx_conn is not None
        with self._tx_conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, request, row_list, template=None, page_size=1000)

    def fetch_cnt(self, cnt: int, request: str, *args) -> List[List[Any]]:
        for retry in itertools.count():
            try:
                if self._conn is None:
                    self._connect()

                with self._cursor() as cursor:
                    cursor.execute(request, *args)
                    return cursor.fetchmany(cnt)

            except BaseException as exc:
                if self._tx_conn is not None:
                    # Got an exception during DB transaction execution
                    #   next steps happens inside run_tx()
                    raise

                self._on_fail_execute(retry, exc)
