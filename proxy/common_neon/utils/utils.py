from __future__ import annotations

import hashlib
import time
from enum import Enum
from typing import Dict, Any, Tuple, List, Set, Union

from ..environment_data import LOG_FULL_OBJECT_INFO


def str_enum(value: Enum) -> str:
    value = str(value)
    idx = value.find('.')
    if idx != -1:
        value = value[idx + 1:]
    return value


def str_fmt_object(obj: Any, skip_prefix=True, name='') -> str:
    def _has_precalculated_str(value: Any) -> bool:
        value = getattr(value, '_str', None)
        return isinstance(value, str) and (len(value) > 0)

    def _lookup_dict_as_value(value: Dict[str, Any]) -> Tuple[bool, str]:
        value = _lookup_dict(value)
        if (not LOG_FULL_OBJECT_INFO) and (len(value) == 0):
            return False, 'None'

        return True, 'dict({' + value + '})'

    def _lookup_str_as_value(value: Union[str, bytes, bytearray]) -> Tuple[bool, str]:
        if (not LOG_FULL_OBJECT_INFO) and (len(value) == 0):
            return False, 'None'

        if isinstance(value, bytes) or isinstance(value, bytearray):
            value = value.hex()
        if (not LOG_FULL_OBJECT_INFO) and (value[:2] in {'0x', '0X'}):
            value = value[2:]
        if (not LOG_FULL_OBJECT_INFO) and (len(value) > 20):
            value = value[:20] + '...'
        return True, "'" + value + "'"

    def _lookup_list_as_value(value: Union[Set[Any], List[Any]]) -> Tuple[bool, str]:
        if LOG_FULL_OBJECT_INFO:
            value = ''
            for item in value:
                has_item, item = _decode_value(item)
                if len(value) > 0:
                    value += ', '
                value += (item if has_item else '?...')
            return True, 'list([' + value + '])'

        elif len(value) > 0:
            return True, f'len({len(value)}, ?...)'

        return False, ''

    def _decode_value(value: Any) -> Tuple[bool, str]:
        if callable(value):
            if LOG_FULL_OBJECT_INFO:
                return True, 'callable(...)'
        elif value is None:
            if LOG_FULL_OBJECT_INFO:
                return True, 'None'
        elif isinstance(value, bool):
            if value or LOG_FULL_OBJECT_INFO:
                return True, str(value)
        elif isinstance(value, Enum):
            return True, str_enum(value)
        elif isinstance(value, list) or isinstance(value, set):
            has_value, value = _lookup_list_as_value(value)
            if has_value:
                return True, value
        elif isinstance(value, str) or isinstance(value, bytes) or isinstance(value, bytearray):
            return _lookup_str_as_value(value)
        elif _has_precalculated_str(value):
            return True, getattr(value, '_str')
        elif hasattr(value, '__dict__'):
            return _lookup_dict_as_value(value.__dict__)
        elif isinstance(value, dict):
            return _lookup_dict_as_value(value)
        elif hasattr(value, '__str__'):
            return True, str(value)
        else:
            return True, value
        return False, '?'

    def _lookup_dict(d: Dict[str, Any]) -> str:
        result: str = ''
        for key, value in d.items():
            if not isinstance(key, str):
                key = str(key)
            if skip_prefix and key.startswith('_'):
                continue
            if key == '_str':
                continue

            has_value, value = _decode_value(value)
            if not has_value:
                continue

            if len(result) > 0:
                result += ', '
            result += key.strip('_') + '=' + value
        return result

    if len(name) == 0:
        name = f'{type(obj)}'
        name = name[name.rfind('.') + 1:-2]
        class_prefix = "<class '"
        if name.startswith(class_prefix):
            name = name[len(class_prefix):]

    if hasattr(obj, '__dict__'):
        content = _lookup_dict(obj.__dict__)
    elif isinstance(obj, dict):
        content = _lookup_dict(obj)
    else:
        content = None

    return f'{name}({content})'


def get_from_dict(src: Dict, *path) -> Any:
    """Provides smart getting values from python dictionary"""
    val = src
    for key in path:
        if not isinstance(val, dict):
            return None
        val = val.get(key)
        if val is None:
            return None
    return val


def gen_unique_id():
    return hashlib.md5((time.time_ns()).to_bytes(16, 'big')).hexdigest()[:7]
