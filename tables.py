from sqlalchemy import Column, Integer, String, Date, DateTime, Float
from database import Base, engine_tokens
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


class IndexTokenTable(Base):
    __tablename__ = 'index_tokens'

    instrument_token = Column(Integer, primary_key=True, index=True)
    tradingsymbol = Column(String(50))
    name = Column(String(50))
    last_update = Column(Date)


class IndexStocksTokenTable(Base):
    __tablename__ = 'index_stocks_tokens'

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    instrument_token = Column(Integer)
    tradingsymbol = Column(String(50))
    name = Column(String(50))
    index = Column(String(50))
    weightage = Column(Float)
    last_update = Column(Date)


# create tables in respective databases
Base.metadata.create_all(bind=engine_tokens, tables=[NseTokenTable.__table__, NseTokenTable.__table__,
                                                     IndexTokenTable.__table__, IndexStocksTokenTable.__table__])
