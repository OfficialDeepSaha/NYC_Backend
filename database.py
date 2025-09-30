import csv
from datetime import datetime
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, String, BigInteger, Text, TIMESTAMP
from sqlalchemy.orm import sessionmaker, declarative_base
from loguru import logger

# ---------------- DATABASE CONFIG ----------------
DATABASE_URL = "postgresql://postgres:BaGEASSspHNlJZKvCXkncMIaaMOQUHQr@ballast.proxy.rlwy.net:30964/railway"

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# CSV file no longer needed - data already loaded to Railway database


# ---------------- DB MODEL ----------------
class Dataset(Base):
    __tablename__ = "nyc_datasets"

    id = Column(String, primary_key=True, index=True)
    name = Column(Text)
    description = Column(Text)
    attribution = Column(Text)
    type = Column(Text)
    data_updated_at = Column(TIMESTAMP)
    page_views_last_week = Column(BigInteger)
    page_views_last_month = Column(BigInteger)
    page_views_total = Column(BigInteger)
    download_count = Column(BigInteger)
    publication_date = Column(TIMESTAMP)
    domain_category = Column(Text)
    domain_tags = Column(Text)
    dataset_information_agency = Column(Text)
    link = Column(Text)


# ---------------- HELPERS ----------------
def init_db():
    Base.metadata.create_all(bind=engine)


def parse_timestamp(value: str):
    if not value or value.strip() == "":
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_int(value: str):
    if not value or value.strip() == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


# Data loading functions removed - data already migrated to Railway database