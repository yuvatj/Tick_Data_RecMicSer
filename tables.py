from sqlalchemy import Column, Integer, String, Date, DateTime, Float
from database import Base, tokens_engine
from datetime import datetime


# Parameters
today = datetime.today().date()


class NfoTokenTable(Base):
    __tablename__ = 'nfo_tokens'

    instrument_token = Column(Integer, primary_key=True, index=True)
    tradingsymbol = Column(String(50))
    lot_size = Column(Integer)
    name = Column(String(50))
    expiry = Column(Date)
    strike = Column(Integer)
    segment = Column(String(50))
    instrument_type = Column(String(50))
    last_update = Column(Date)


class NseTokenTable(Base):
    __tablename__ = 'nse_tokens'

    instrument_token = Column(Integer, primary_key=True, index=True)
    tradingsymbol = Column(String(50))
    lot_size = Column(Integer)
    name = Column(String(50))
    last_update = Column(Date)


# create tables in respective databases
Base.metadata.create_all(bind=tokens_engine, tables=[NseTokenTable.__table__, NseTokenTable.__table__])
