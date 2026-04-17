
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from .config import SQLITE_URL

engine = create_engine(SQLITE_URL, connect_args={"check_same_thread": False}, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()
