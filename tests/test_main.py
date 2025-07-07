import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData, select, func
import json
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent

def load_settings():
    settings_path = ROOT_DIR / 'local.settings.json'
    try:
        with open(settings_path) as f:
            settings = json.load(f)
            return settings.get('Values', {})
    except:
        raise FileNotFoundError("local.settings.json not found in")
    
settings = load_settings()

POSTGRES_CONN_STRING = (
    f"postgresql://{settings['PG_USER']}:{settings['PG_PASSWORD']}"
    f"@{settings['PG_HOST']}:{settings['PG_PORT']}/{settings['PG_DB']}"
)

metadata = MetaData()
engine = create_engine(POSTGRES_CONN_STRING)
Session = sessionmaker(bind=engine)
def test_count():
    try:
        view_county = Table("view_county", metadata, autoload_with=engine)
        stmt = select(func.count()).select_from(view_county)
        with Session() as session:
            result = session.execute(stmt).scalar()
        assert result == 100_000, f"Expected 100,000 rows, found {result}"
    except Exception as e:
        pytest.fail(f"Check failed: {str(e)}")
    
def test_duplicates():
    try:
        metadata = MetaData()
        view_county = Table("view_county", metadata, autoload_with=engine)
        
        stmt = (
            select(view_county.c.npi, func.count())
            .select_from(view_county)
            .group_by(view_county.c.npi)
            .having(func.count() > 1)
        )
        with Session() as session:
            duplicates = session.execute(stmt).all()
            
        assert not duplicates, f"Duplicate npi entries found: {duplicates}"
    
    except Exception as e:
        pytest.fail(f"Test failed with errors: {str(e)}")
    

if __name__ == "__main__":
    test_duplicates()