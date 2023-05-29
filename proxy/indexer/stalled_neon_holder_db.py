from typing import List, Any, Iterator

from ..common_neon.db.base_db_table import BaseDBTable
from ..common_neon.db.db_connect import DBConnection

from .indexed_objects import NeonIndexedHolderInfo


class StalledNeonHoldersDB(BaseDBTable):
    def __init__(self, db: DBConnection):
        super().__init__(
            db,
            table_name='stalled_neon_holders',
            column_list=[
                'account', 'neon_sig', 'block_slot',
                'start_block_slot', 'last_block_slot',
                'data_size', 'data'
            ],
            key_list=['account', 'neon_sig']
        )

        self._select_request = f'''
            SELECT {', '.join(['a.' + c for c in self._column_list])}
              FROM {self._table_name} AS a
             WHERE a.block_slot < %s
               AND a.start_block_slot < %s
        '''

        self._delete_request = f'''
            DELETE FROM {self._table_name}
             WHERE block_slot != %s
        '''

    def set_holder_list(self, block_slot: int, iter_neon_holder: Iterator[NeonIndexedHolderInfo]) -> None:
        row_list: List[List[Any]] = list()
        for holder in iter_neon_holder:
            value_list: List[Any] = list()
            for idx, column in enumerate(self._column_list):
                if column == 'block_slot':
                    value_list.append(block_slot)
                elif column == 'data':
                    value_list.append(holder.data.hex())
                elif hasattr(holder, column):
                    value_list.append(getattr(holder, column))
                else:
                    raise RuntimeError(f'Wrong usage {self._table_name}: {idx} -> {column}!')
            row_list.append(value_list)

        self._insert_row_list(row_list)

        self._db.update_row(self._delete_request, [block_slot])

    def get_holder_list(self, block_slot: int) -> List[NeonIndexedHolderInfo]:
        row_list = self._db.fetch_all(self._select_request, [block_slot, block_slot])

        holder_list: List[NeonIndexedHolderInfo] = list()
        for value_list in row_list:
            key = NeonIndexedHolderInfo.Key(
                self._get_column_value('account', value_list),
                self._get_column_value('neon_sig', value_list)
            )
            chunk = NeonIndexedHolderInfo.DataChunk(
                offset=0,
                length=self._get_column_value('data_size', value_list),
                data=bytes.fromhex(self._get_column_value('data', value_list))
            )
            holder = NeonIndexedHolderInfo(key)
            holder.set_start_block_slot(self._get_column_value('start_block_slot', value_list))
            holder.set_last_block_slot(self._get_column_value('last_block_slot', value_list))
            holder.add_data_chunk(chunk)

            holder_list.append(holder)
        return holder_list
