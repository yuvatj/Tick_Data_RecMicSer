# Python Standard Library
import json
import time
import datetime
from dataclasses import asdict

import sqlalchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy.inspection import inspect
from sqlalchemy import Column, Integer, String, Date, DateTime, Float, MetaData, JSON
from sqlalchemy.orm.exc import NoResultFound

# Local Library imports
import models
import tables
from utility import Utility
from kite_websocket import TickData
from kite_login import LoginCredentials
from active_symbols import NseActiveSymbols, NfoActiveSymbols, IndexActiveSymbols
from database import Base, SessionLocalTokens, engine_day_data_index, engine_candle_data_index
from database import engine_candle_data_nse, engine_candle_data_nfo, engine_day_data_nse, engine_day_data_nfo

# Parameters
ut = Utility()
tick = TickData()
user = LoginCredentials()
today = datetime.datetime.today().date()
token_db = SessionLocalTokens()

log = user.credentials


def add_token(model: models.NfoTokenModel | models.NseTokenModel | models.IndexTokenModel):
    """ Function to add a token data to the database only related to NSE and NFO"""

    # Parameters
    _token = None

    # Connect to the database
    db = token_db

    # Convert the model to a dictionary and delete the "exchange" key
    my_dict = asdict(model)
    del my_dict['exchange']
    model_dict = my_dict

    try:
        # Create a new token and add it to the database
        if model.exchange == 'NSE':
            _token = tables.NseTokenTable(**model_dict)

        elif model.exchange == 'NFO':
            _token = tables.NfoTokenTable(**model_dict)

        elif model.exchange == 'INDEX':
            _token = tables.IndexTokenTable(**model_dict)

        db.add(_token)
        db.commit()
        db.refresh(_token)

    except IntegrityError as e:
        # If there is an IntegrityError, rollback the transaction and handle the duplicate entry error
        db.rollback()
        if 'Duplicate entry' in str(e):
            # If the model's exchange is NFO, update the existing NfoTable object
            if model.exchange == 'NFO':
                existing_token = db.query(tables.NfoTokenTable) \
                    .filter(tables.NfoTokenTable.instrument_token == model.instrument_token).first()

                # Update the values of the existing token
                existing_token.name = model.name
                existing_token.expiry = model.expiry
                existing_token.strike = model.strike
                existing_token.instrument_type = model.instrument_type
                existing_token.segment = model.segment
                existing_token.lot_size = model.lot_size
                existing_token.trading_symbol = model.tradingsymbol
                existing_token.last_update = model.last_update
                existing_token.position = model.position

                # Add the updated token to the database
                db.add(existing_token)
                db.commit()

            # If the model's exchange is NSE, update the existing NseTable object
            elif model.exchange == 'NSE':
                existing_token = db.query(tables.NseTokenTable) \
                    .filter(tables.NseTokenTable.instrument_token == model.instrument_token).first()

                # Update the values of the existing token
                existing_token.name = model.name
                existing_token.trading_symbol = model.tradingsymbol
                existing_token.bank_nifty = model.bank_nifty
                existing_token.nifty = model.nifty
                existing_token.fin_nifty = model.fin_nifty
                existing_token.last_update = model.last_update

                # Add the updated token to the database
                db.add(existing_token)
                db.commit()

            # If the model's exchange is NSE, update the existing NseTable object
            elif model.exchange == 'INDEX':
                existing_token = db.query(tables.IndexTokenTable) \
                    .filter(tables.IndexTokenTable.instrument_token == model.instrument_token).first()

                # Update the values of the existing token
                existing_token.name = model.name
                existing_token.trading_symbol = model.tradingsymbol
                existing_token.last_update = model.last_update

                # Add the updated token to the database
                db.add(existing_token)
                db.commit()

    finally:
        # Close the database connection
        db.close()


def get_tokens(exchange: str) -> list:
    """Fetch token numbers for instruments NSE, NFO. INDEX

    Args:
        exchange: 'NSE', fetch token numbers for NSE instruments.
        exchange: 'NFO', fetch token numbers for NFO instruments.
        exchange: 'INDEX', fetch token numbers for INDEX instruments.

    Returns:
        A list of token numbers updated today.
    """

    # Parameter
    token_table = None
    exchange = exchange.upper()

    # Get the token table model.
    if exchange == 'NSE':
        token_table = tables.NseTokenTable

    elif exchange == 'NFO':
        token_table = tables.NfoTokenTable

    elif exchange == 'INDEX':
        token_table = tables.IndexTokenTable

    # Query the database for token numbers.
    tokens_data = token_db.query(token_table).filter(token_table.last_update == today)

    # Convert the query results to a list of token numbers.
    result = [item.instrument_token for item in tokens_data]

    # Close tokens_db
    token_db.close()

    # Return the list of token numbers.
    return result


def create_table_in_candle_data_db(exchange: str):

    """ Because this directory is under progress the table schema for NSE and NFO are kept differently,
        In future iteration this issue will be resolved."""

    # Parameters
    _engine_ = None
    _tokens_ = None  # [256265, 257801, 260105]
    _token_names = None  # ['token_256265', 'token_257801', 'token_260105']
    exchange = exchange.upper()  # 'NSE', 'NFO', 'INDEX'

    if exchange == 'NSE':
        _tokens_ = get_tokens('NSE')
        _engine_ = engine_candle_data_nse
        _token_names = [f"token_{token}" for token in _tokens_]

    elif exchange == 'NFO':
        _tokens_ = get_tokens('NFO')
        _engine_ = engine_candle_data_nfo
        _token_names = [f"token_{token}" for token in _tokens_]

    elif exchange == 'INDEX':
        _tokens_ = get_tokens('INDEX')
        _engine_ = engine_candle_data_index
        _token_names = [f"token_{token}" for token in _tokens_]

    # Get a list of all the table names
    with _engine_.connect() as conn:
        inspector = inspect(_engine_)
        table_names = inspector.get_table_names()

    # Create a list of tables for the tokens
    tables_list = []
    for token in _token_names:
        if token not in table_names:
            if exchange == 'NSE':
                # Define the table for the NSE token
                class NseInstrumentTable(Base):
                    __tablename__ = f"{token}"

                    time_stamp = Column(DateTime, primary_key=True)
                    open = Column(Float)
                    high = Column(Float)
                    low = Column(Float)
                    close = Column(Float)
                    volume = Column(Integer)
                    liquidity_profile = Column(JSON)
                    volume_profile = Column(JSON)
                    order_profile = Column(JSON)
                    candle_data = Column(JSON)

                # Create the table in the database
                tables_list.append(NseInstrumentTable.__table__)

            elif exchange == 'NFO':
                # Define the table for the NFO token
                class NfoInstrumentTable(Base):
                    __tablename__ = f"{token}"

                    time_stamp = Column(DateTime, primary_key=True)
                    open = Column(Float)
                    high = Column(Float)
                    low = Column(Float)
                    close = Column(Float)
                    volume = Column(Integer)
                    liquidity_profile = Column(JSON)
                    volume_profile = Column(JSON)
                    order_profile = Column(JSON)
                    candle_data = Column(JSON)

                # Create the table in the database
                tables_list.append(NfoInstrumentTable.__table__)

            elif exchange == 'INDEX':
                # Define the table for the NFO token
                class IndexInstrumentTable(Base):
                    __tablename__ = f"{token}"

                    time_stamp = Column(DateTime, primary_key=True)
                    open = Column(Float)
                    high = Column(Float)
                    low = Column(Float)
                    close = Column(Float)
                    cash_profile = Column(JSON)
                    future_profile = Column(JSON)
                    option_profile = Column(JSON)
                    candle_data = Column(JSON)

                # Create the table in the database
                tables_list.append(IndexInstrumentTable.__table__)

    # Create all the tables in the database
    Base.metadata.create_all(_engine_, tables=tables_list)

    # Print a message for each table that was created
    for table in tables_list:
        print(f'Table for token {table.name} created')


def create_table_in_day_data_db(exchange: str):
    # Parameters
    _engine_ = None
    _tokens_ = None  # [256265, 257801, 260105]
    _token_names = None  # ['token_256265', 'token_257801', 'token_260105']
    exchange = exchange.upper()  # 'NSE', 'NFO', 'INDEX'

    if exchange == 'NSE':
        _tokens_ = get_tokens('NSE')
        _engine_ = engine_day_data_nse
        _token_names = [f"token_{token}" for token in _tokens_]

    elif exchange == 'NFO':
        _tokens_ = get_tokens('NFO')
        _engine_ = engine_day_data_nfo
        _token_names = [f"token_{token}" for token in _tokens_]

    elif exchange == 'INDEX':
        _tokens_ = get_tokens('INDEX')
        _engine_ = engine_day_data_index
        _token_names = [f"token_{token}" for token in _tokens_]

    # Get a list of all the table names
    with _engine_.connect() as conn:
        inspector = inspect(_engine_)
        table_names = inspector.get_table_names()

    # Create a list of tables for the tokens
    tables_list = []
    for token in _token_names:
        if token not in table_names:
            if exchange == 'NSE':
                # Define the table for the NSE token
                class NseInstrumentTable(Base):
                    __tablename__ = f"{token}"
                    __table_args__ = {'extend_existing': True}

                    time_stamp = Column(DateTime, primary_key=True)
                    open = Column(Float)
                    high = Column(Float)
                    low = Column(Float)
                    close = Column(Float)
                    volume = Column(Integer)
                    liquidity_profile = Column(JSON)
                    volume_profile = Column(JSON)
                    order_profile = Column(JSON)
                    candle_data = Column(JSON)

                # Create the table in the database
                tables_list.append(NseInstrumentTable.__table__)

            elif exchange == 'NFO':
                # Define the table for the NFO token
                class NfoInstrumentTable(Base):
                    __tablename__ = f"{token}"
                    __table_args__ = {'extend_existing': True}

                    time_stamp = Column(DateTime, primary_key=True)
                    open = Column(Float)
                    high = Column(Float)
                    low = Column(Float)
                    close = Column(Float)
                    volume = Column(Integer)
                    liquidity_profile = Column(JSON)
                    volume_profile = Column(JSON)
                    order_profile = Column(JSON)
                    candle_data = Column(JSON)

                # Create the table in the database
                tables_list.append(NfoInstrumentTable.__table__)

            elif exchange == 'INDEX':
                # Define the table for the NFO token
                class IndexInstrumentTable(Base):
                    __tablename__ = f"{token}"
                    __table_args__ = {'extend_existing': True}

                    time_stamp = Column(DateTime, primary_key=True)
                    open = Column(Float)
                    high = Column(Float)
                    low = Column(Float)
                    close = Column(Float)
                    cash_profile = Column(JSON)
                    future_profile = Column(JSON)
                    option_profile = Column(JSON)
                    candle_data = Column(JSON)

                # Create the table in the database
                tables_list.append(IndexInstrumentTable.__table__)

    # Create all the tables in the database
    Base.metadata.create_all(_engine_, tables=tables_list)

    # Print a message for each table that was created
    for table in tables_list:
        print(f'Table for token {table.name} created')


def preprocessing():
    print("Tokens data preprocessing started")

    # Get the current time.
    time_live = datetime.datetime.now().time().replace(microsecond=0)

    start_time = datetime.datetime.combine(datetime.date.today(), datetime.time(9, 10))

    # Calculate the delay until the start time
    if time_live < start_time.time():
        time_diff = (start_time - datetime.datetime.today()).seconds
        print(f"Waiting {time_diff} seconds.")
        time.sleep(time_diff + 1)

    nse_tokens = NseActiveSymbols()
    nfo_tokens = NfoActiveSymbols()
    index_tokens = IndexActiveSymbols()

    # Merge all tokens into a single list
    tokens_combined = nse_tokens.to_tokens() + nfo_tokens.to_tokens() + index_tokens.to_tokens()

    # add the tokens to the database
    for token_item in tokens_combined:
        add_token(token_item)

    # Create missing tables of candles database
    create_table_in_candle_data_db(exchange='NSE')
    create_table_in_candle_data_db(exchange='NFO')
    create_table_in_candle_data_db(exchange='INDEX')

    # Create missing tables of day database
    create_table_in_day_data_db(exchange='NSE')
    create_table_in_day_data_db(exchange='NFO')
    create_table_in_day_data_db(exchange='INDEX')


if __name__ == '__main__':
    # Use this to create missing tables daily
    preprocessing()

    # tokens = get_tokens(exchange='INDEX')
    #
    # print(tokens)










