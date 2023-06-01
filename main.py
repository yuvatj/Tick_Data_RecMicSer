# Python Standard Library
import json
from datetime import datetime
from dataclasses import dataclass, asdict

import sqlalchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy.inspection import inspect
from sqlalchemy import Column, Integer, DateTime, Float
from sqlalchemy.orm.exc import NoResultFound


# Local Library imports
import models
import tables
from utility import Utility
from kite_websocket import TickData
from active_symbols import ActiveSymbols
from token_app.preset import predefined_requests
from database import Base, SessionLocalTokens
from database import engine_candle_data_nse, engine_candle_data_nfo, engine_candle_data_index

# Parameters
ut = Utility()
tick = TickData()
activeSymbols = ActiveSymbols()
today = datetime.today().date()
token_db = SessionLocalTokens()


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
                existing_token = db.query(tables.NfoTokenTable)\
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
                existing_token.lot_size = model.lot_size
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

    # Return the list of token numbers.
    return result


def create_table_for_instruments(exchange: str):

    # Parameters
    _engine_ = None
    _tokens_ = None                 # [256265, 257801, 260105]
    _token_names = None             # ['token_256265', 'token_257801', 'token_260105']
    exchange = exchange.upper()     # 'NSE', 'NFO', 'INDEX'

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
                    net_long = Column(Float)
                    net_short = Column(Float)
                    per_trade_value = Column(Float)

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
                    net_long = Column(Float)
                    net_short = Column(Float)
                    per_trade_value = Column(Float)
                    open_interest = Column(Integer)

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


def get_index_stocks():
    index_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']
    stock_list = []

    for item in index_list:
        try:
            with open(f"{item}.json", "r") as json_file:
                data = json.load(json_file)
                symbol_count = len(data)

                # Create a model to fetch data for NSE symbols
                nse_model = predefined_requests("token_NSE_ALL")
                nse_model.name = tuple(data.keys())
                nse_tokens = activeSymbols.get_symbol_data(nse_model)
                token = [item.tradingsymbol for item in nse_tokens]

                if len(token) == symbol_count:
                    # Append the keys to stock_list
                    stock_list.extend(data.keys())
                else:
                    # Raise an exception if the number of symbols fetched does not match the expected count

                    # Convert the lists to sets
                    set1 = set(token)
                    set2 = set(data.keys())

                    # Get the items that are present in set1 but not in set2
                    unique_items = set2 - set1

                    # Convert the result back to a list
                    result = list(unique_items)
                    raise NameError(f"{item}.json Stocks {result} do not match")

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found at the {item}.json location")

    # Get the unique stocks by converting the list to a set and then back to a list
    unique_stocks = list(set(stock_list))
    return unique_stocks


def preprocessing():

    decision = False # keep it False
    while decision is False:

        # Create a model to fetch data for NSE symbols
        nse_model = predefined_requests("token_NSE_ALL")
        # Add list of NSE stocks to name
        nse_stocks = tuple(activeSymbols.get_symbol_data(predefined_requests("stocks_NFO")))
        index_stocks = tuple(get_index_stocks())

        combined_stocks = nse_stocks + index_stocks
        unique_stocks = tuple(set(combined_stocks))

        nse_model.name = unique_stocks
        nse_tokens = activeSymbols.get_symbol_data(nse_model)

        # Fetch data for Futures NIFTY and BANKNIFTY
        futures_model = predefined_requests('futures_NI_BN')
        fut_tokens = activeSymbols.get_symbol_data(futures_model)

        # Fetch data for Options for BANKNIFTY
        options_model = predefined_requests('options_BN')
        # Take input for BANKNIFTY spot pre-open price
        options_model.strike = int(input("Enter BANKNIFTY spot pre-open price example: 44_200: "))
        banknifty_opt_tokens = activeSymbols.get_symbol_data(options_model)

        # Fetch data for INDEX symbols
        symbol_model = predefined_requests("INDEX")
        index_tokens = activeSymbols.get_symbol_data(symbol_model)

        # Merge all tokens into a single list
        tokens_combined = nse_tokens + fut_tokens + banknifty_opt_tokens + index_tokens

        # Print the number of symbols for Futures, Options, and Stocks
        print(f"Futures:{len(fut_tokens)}\nOptions:{len(banknifty_opt_tokens)}\n"
              f"Stocks:{len(nse_tokens)}\nIndex:{len(index_tokens)}")

        decision = True if input("Press 'Y' to continue or 'N' to redo: ").upper() == 'Y' else False

        if decision:
            # add the tokens to the database
            for token_item in tokens_combined:
                add_token(token_item)

    # Create missing tables of NSE
    create_table_for_instruments(exchange='NSE')

    # Create missing tables of NFO
    create_table_for_instruments(exchange='NFO')

    # Create missing tables of INDEX
    create_table_for_instruments(exchange='INDEX')


if __name__ == '__main__':
    preprocessing()