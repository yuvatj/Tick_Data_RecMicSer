# Python Standard Library
import json
import time
import datetime
from dataclasses import asdict
from kiteconnect import KiteConnect

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
from active_symbols import ActiveSymbols
from token_app.preset import predefined_requests
from database import Base, SessionLocalTokens, engine_tokens
from database import engine_candle_data_nse, engine_candle_data_nfo, engine_candle_data_index

# Parameters
ut = Utility()
tick = TickData()
user = LoginCredentials()
activeSymbols = ActiveSymbols()
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

            # If the model's exchange is INDEX, update the existing IndexTable object
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


def create_table_for_instruments(exchange: str):
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
                    volume_long = Column(Integer)
                    volume_short = Column(Integer)
                    net_long = Column(Float)
                    net_short = Column(Float)
                    per_trade = Column(Float)
                    wa_long = Column(Float)
                    wa_short = Column(Float)
                    vwap = Column(Float)
                    quality = Column(Integer)
                    liquidity_profile = Column(JSON)
                    volume_profile = Column(JSON)

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
                    volume_long = Column(Integer)
                    volume_short = Column(Integer)
                    net_long = Column(Float)
                    net_short = Column(Float)
                    per_trade = Column(Float)
                    wa_long = Column(Float)
                    wa_short = Column(Float)
                    vwap = Column(Float)
                    total_value = Column(Float)
                    value_today = Column(Float)
                    open_interest = Column(Integer)
                    quality = Column(Integer)
                    liquidity_profile = Column(JSON)
                    volume_profile = Column(JSON)

                # Create the table in the database
                tables_list.append(NfoInstrumentTable.__table__)

            elif exchange == 'INDEX':
                # Define the table for the INDEX token
                class IndexInstrumentTable(Base):
                    __tablename__ = f"{token}"

                    time_stamp = Column(DateTime, primary_key=True)
                    open = Column(Float)
                    high = Column(Float)
                    low = Column(Float)
                    close = Column(Float)

                # Create the table in the database
                tables_list.append(IndexInstrumentTable.__table__)

    # Create all the tables in the database
    Base.metadata.create_all(_engine_, tables=tables_list)

    # Print a message for each table that was created
    for table in tables_list:
        print(f'Table for token {table.name} created')


def add_index_weightage(model: models.NseTokenModel):
    """ This function is capable of handling missing symbols of indexes and assigning weightage values """

    # Parameters
    index_instruments = ['BANKNIFTY', 'NIFTY', 'FINNIFTY']
    model_symbols = {item.tradingsymbol for item in model}  # Use set for faster membership checking

    for index_symbol in index_instruments:

        stock_list = []
        try:
            with open(f"{index_symbol}.json", "r") as json_file:
                data = json.load(json_file)

                stock_list.extend(data.keys())

        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found at the {index_symbol}.json location") from e

        missing_symbols = [item for item in stock_list if item not in model_symbols]

        if missing_symbols:
            raise ValueError(f"Missing items from {index_symbol}: {missing_symbols}")

        for nse_model in model:
            if nse_model.tradingsymbol in stock_list:
                # Use a dictionary to map index symbols to corresponding attributes
                index_attributes = {
                    'BANKNIFTY': 'bank_nifty',
                    'NIFTY': 'nifty',
                    'FINNIFTY': 'fin_nifty'
                }

                setattr(nse_model, index_attributes[index_symbol], data[nse_model.tradingsymbol])

    return model


def round_to_nearest_multiple(number, multiple):
    return round(number / multiple) * multiple


def get_index_premarket_price():

    kite = KiteConnect(api_key=log.api_key)
    kite.set_access_token(log.access_token)

    # List of instrument tokens for the indices
    instrument_tokens = ['NSE:NIFTY 50', 'NSE:NIFTY BANK', 'NSE:NIFTY FIN SERVICE']

    # Get quotes for the indices
    quotes = kite.quote(instrument_tokens)

    # Extract required information from quotes
    index_quotes = {}
    for token in instrument_tokens:
        index_quotes[token] = {
            "last_price": quotes[token]["last_price"],
            "close": quotes[token]["ohlc"]["close"]
        }

    return index_quotes


def preprocessing():

    decision = False  # keep it False
    counter = 0
    max_attempts = 5

    while decision is False:

        time.sleep(5)  # Sleep for 10 seconds before the next attempt

        counter += 1

        print("Tokens data preprocessing started")

        # Fetch data for INDEX symbols
        symbol_model = predefined_requests("INDEX")
        index_tokens = activeSymbols.get_symbol_data(symbol_model)

        # Fetch data for Futures NIFTY and BANKNIFTY
        futures_model = predefined_requests('futures_NI_BN')
        fut_tokens = activeSymbols.get_symbol_data(futures_model)

        # Create a model to fetch data for NSE symbols
        nse_symbol_model = predefined_requests("token_NSE_ALL")
        # Add list of NSE stocks to name
        nse_stocks = tuple(activeSymbols.get_symbol_data(predefined_requests("stocks_NFO")))
        nse_symbol_model.name = nse_stocks
        nse_model = activeSymbols.get_symbol_data(nse_symbol_model)
        nse_tokens = add_index_weightage(nse_model)

        # Get the current time.
        time_live = datetime.datetime.now().time().replace(microsecond=0)

        start_time = datetime.datetime.combine(datetime.date.today(), datetime.time(9, 10))

        # Calculate the delay until the start time
        if time_live < start_time.time():
            time_diff = (start_time - datetime.datetime.today()).seconds
            print(f"Waiting {time_diff} seconds.")
            time.sleep(time_diff + 1)

        # Fetch data for Options for BANKNIFTY
        options_model = predefined_requests('options_BN')
        banknifty_last_price = get_index_premarket_price()['NSE:NIFTY BANK']['last_price']
        options_model.strike = round_to_nearest_multiple(banknifty_last_price, 100)
        banknifty_opt_tokens = activeSymbols.get_symbol_data(options_model)

        print(f"Bank Nifty ltp: {banknifty_last_price}")

        # Merge all tokens into a single list
        tokens_combined = nse_tokens + fut_tokens + banknifty_opt_tokens + index_tokens

        index = len(index_tokens) == 3
        futures = len(fut_tokens) == 6
        fno_stocks = len(nse_tokens) > 170
        options = len(banknifty_opt_tokens) == 62

        requirements = [index, futures, fno_stocks, options]

        # Print the number of symbols for Futures, Options, and Stocks
        print(f"Futures:{len(fut_tokens)}\nOptions:{len(banknifty_opt_tokens)}\n"
              f"Stocks:{len(nse_tokens)}\nIndex:{len(index_tokens)}")

        if all(requirements):
            decision = True

            # add the tokens to the database
            for token_item in tokens_combined:
                add_token(token_item)

        if counter >= max_attempts:
            raise ValueError("Failed to fetch required data after multiple attempts.")

    # Create missing tables of NSE
    create_table_for_instruments(exchange='NSE')

    # Create missing tables of NFO
    create_table_for_instruments(exchange='NFO')

    # Create missing tables of INDEX
    create_table_for_instruments(exchange='INDEX')


if __name__ == '__main__':
    # Use this to create missing tables daily
    preprocessing()

    # This function will delete "index_stocks_tokens" table in tokens database
    # and recreates the table with new data. Use only you update the index.json files
    # create_index_stocks()








