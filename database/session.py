from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session


class DBSession:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url, pool_pre_ping=True, future=True)
        self.SessionLocal = sessionmaker(bind=self.engine, class_=Session, expire_on_commit=False)

    @contextmanager
    def session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def tenant_session(self, tenant_schema: str):
        """
        Opens a session with the PostgreSQL search_path set
        to the tenant schema + public.
        """
        session = self.SessionLocal()
        try:
            session.exec(text(f"SET search_path TO {tenant_schema.lower()}, public"))
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
