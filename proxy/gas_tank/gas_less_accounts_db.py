from typing import Iterator, Any, List

from .gas_tank_types import GasLessPermit

from ..common_neon.address import NeonAddress

from ..indexer.base_db import BaseDB


class GasLessAccountsDB(BaseDB):
    def __init__(self):
        super().__init__(
            table_name='gas_less_accounts',
            column_list=[
                'address', 'contract', 'nonce', 'block_slot', 'neon_sig'
            ]
        )

    def has_gas_less_tx_permit(self, address: NeonAddress) -> bool:
        request = f'''
            SELECT a.address
              FROM {self._table_name} AS a
             WHERE a.address = %s AND a.nonce = 0 AND a.contract IS NULL
             LIMIT 1
        '''
        with self._conn.cursor() as cursor:
            cursor.execute(request, (str(address),))
            value_list = cursor.fetchone()
            return value_list is not None

    def add_gas_less_permit_list(self, cursor: BaseDB.Cursor, permit_list: Iterator[GasLessPermit]) -> None:
        row_list: List[List[Any]] = list()
        for permit in permit_list:
            row_list.append([
                str(permit.account), str(permit.contract), permit.nonce, permit.block_slot, permit.neon_sig
            ])

        self._insert_batch(cursor, row_list)
