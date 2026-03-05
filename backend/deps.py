# # backend/deps.py
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from config import JOBS_DB, INGEST_META_DB

# print(JOBS_DB)
# print(JOBS_DB.exists())

# jobs_engine = create_engine(f"sqlite:///{JOBS_DB}", connect_args={"check_same_thread": False})
# JobsSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=jobs_engine)

# ingest_engine = create_engine(f"sqlite:///{INGEST_META_DB}", connect_args={"check_same_thread": False})
# IngestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ingest_engine)


# def get_db():
#     if not JOBS_DB.exists():
#         raise RuntimeError(
#             f"jobs.db not found at {JOBS_DB}. "
#             "Start the worker once to initialize it."
#         )
#     db = JobsSessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()


# def get_ingest_db():
#     db = IngestSessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql+psycopg2://postgres:nik_admin_434@localhost:5432/postgres"

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()