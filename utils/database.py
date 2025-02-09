import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, JSON, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pathlib

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create data directory if it doesn't exist
data_dir = pathlib.Path("data")
try:
    data_dir.mkdir(exist_ok=True)
    logger.info(f"Data directory confirmed at: {data_dir}")
except Exception as e:
    logger.error(f"Failed to create data directory: {e}")
    raise

# Get database URL from environment variables, default to SQLite
DATABASE_URL = os.getenv('DATABASE_URL', f'sqlite:///{data_dir}/analyses.db')
logger.info(f"Using database: {DATABASE_URL}")

# Create SQLAlchemy engine
try:
    if DATABASE_URL.startswith("sqlite"):
        engine = create_engine(
            DATABASE_URL, 
            connect_args={"check_same_thread": False}  # Needed for SQLite
        )
    else:
        engine = create_engine(DATABASE_URL)
    logger.info("Database engine created successfully")
except Exception as e:
    logger.error(f"Failed to create database engine: {e}")
    raise

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base = declarative_base()

class CodeTemplate(Base):
    __tablename__ = "code_templates"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    language = Column(String)
    template = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Analysis(Base):
    __tablename__ = "analyses"
    
    id = Column(Integer, primary_key=True, index=True)
    config = Column(JSON)
    python_code = Column(String)
    r_code = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    description = Column(String)

# Create tables
try:
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Failed to create database tables: {e}")
    raise

def get_db():
    """Database session generator with error handling"""
    db = SessionLocal()
    try:
        logger.debug("Database session created")
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        raise
    finally:
        logger.debug("Closing database session")
        db.close()
