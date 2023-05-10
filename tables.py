from sqlalchemy import Column, Integer, String, Date, DateTime, Float
from database import Base, tokens_engine, nfo_tick_engine, nse_tick_engine
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


class NfoInstrumentTable(Base):
    __tablename__ = 'nfo_example'

    time_stamp = Column(DateTime, primary_key=True)
    price = Column(Float)
    average_price = Column(Float)
    total_buy_qty = Column(Integer)
    total_sell_qty = Column(Integer)
    traded_volume = Column(Integer)
    open_interest = Column(Integer)


# create tables in respective databases
Base.metadata.create_all(bind=nfo_tick_engine, tables=[NfoInstrumentTable.__table__])


class NseInstrumentTable(Base):
    __tablename__ = 'nse_example'

    time_stamp = Column(DateTime, primary_key=True)
    price = Column(Float)
    average_price = Column(Float)
    total_buy_qty = Column(Integer)
    total_sell_qty = Column(Integer)
    traded_volume = Column(Integer)


# create tables in respective databases
Base.metadata.create_all(bind=nse_tick_engine, tables=[NseInstrumentTable.__table__])


