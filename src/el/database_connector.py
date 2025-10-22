from sqlalchemy import create_engine
from abc import ABC
import os

class DatabaseConnector(ABC):
    """Base class to manage database connections with SQLAlchemy"""

    def __init__(self):
        self.db_url = self._build_database_url()
        self.engine = self._create_engine()

    def get_engine(self):
        """Return the SQLAlchemy engine."""
        return self.engine

    # ---------------------------------------------------------
    #                   Private helpers
    # ---------------------------------------------------------
    def _build_database_url(self) -> str:
        """Get DB URL from environment variables"""
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("Can't get the database url from the environement variabels")
        return db_url

    def _create_engine(self):
        return create_engine(
            self.db_url,
            echo=False,
            future=True,
            pool_pre_ping=True,  # check connection before reuse
        )

