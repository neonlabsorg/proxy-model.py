import os

from typing import Dict, Any


class DBConfig:
    def __init__(self):
        self._postgres_host = os.environ.get('POSTGRES_HOST', '')
        self._postgres_db = os.environ.get('POSTGRES_DB', '')
        self._postgres_user = os.environ.get('POSTGRES_USER', '')
        self._postgres_password = os.environ.get('POSTGRES_PASSWORD', '')
        self._postgres_timeout = int(os.environ.get('POSTGRES_TIMEOUT', '0'), 10)

    @property
    def postgres_host(self) -> str:
        assert len(self._postgres_host)
        return self._postgres_host

    @property
    def postgres_db(self) -> str:
        assert len(self._postgres_db)
        return self._postgres_db

    @property
    def postgres_user(self) -> str:
        assert len(self._postgres_user)
        return self._postgres_user

    @property
    def postgres_password(self) -> str:
        return self._postgres_password

    @property
    def postgres_timeout(self):
        return self._postgres_timeout

    def as_dict(self) -> Dict[str, Any]:
        return {
            # Don't print private configuration
            # 'POSTGRES_HOST': self.postgres_host,
            # 'POSTGRES_DB': self.postgres_db,
            # 'POSTGRES_USER': self.postgres_user,
            # 'POSTGRES_PASSWORD': self.postgres_password

            'POSTGRES_TIMEOUT': self.postgres_timeout,
        }
