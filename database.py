import contextlib
from typing import Iterator, Any

from sqlalchemy import create_engine, Connection, URL, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base

import tomllib as tl

with open('config.toml', 'rb') as f:
    config = tl.load(f)

url = f"sqlite:///{config['DATABASE']['file']}"
Base = declarative_base()

class DatabaseSessionManager:
    def __init__(self, host: str | URL, engine_kwargs: dict[str, Any] = {}):
        self._engine = create_engine(host, **engine_kwargs)
        self._sessionmaker = sessionmaker(autocommit=False, bind=self._engine)

    def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        self._engine.dispose()
        self._engine = None
        self._sessionmaker = None

    @contextlib.contextmanager
    def connect(self) -> Iterator[Connection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        connection = self._engine.connect()
        try:
            yield connection
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextlib.contextmanager
    def session(self) -> Iterator[Session]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


sessionmanager = DatabaseSessionManager(url)

def get_db_session():
    with sessionmanager.session() as session:
        yield session
        
def test():
    with sessionmanager.connect() as conn:
        result = conn.execute(text("select 1"))
        print(result.fetchone())
        