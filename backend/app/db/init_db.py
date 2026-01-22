from sqlmodel import SQLModel
from app.db.engine import get_engine


def init_db():
    SQLModel.metadata.create_all(get_engine())