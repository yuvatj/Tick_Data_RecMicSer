# Python Standard Library
from datetime import datetime
from dataclasses import dataclass, asdict
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import Column, Integer, DateTime, Float
from sqlalchemy.orm.exc import NoResultFound


# Local Library imports
import models
import tables
from utility import Utility
from kite_websocket import TickData
from active_symbols import ActiveSymbols
from token_app.preset import predefined_requests
from database import Base, TokensSessionLocal, nse_tick_engine, nfo_tick_engine


# Parameters
ut = Utility()
tick = TickData()
activeSymbols = ActiveSymbols()
today = datetime.today().date()
token_db = TokensSessionLocal()


def add_token(model: models.NfoTokenModel | models.NseTokenModel):
    """ Function to add a token data to the database"""

    # Connect to the database
    db = TokensSessionLocal()

    # Convert the model to a dictionary and delete the "exchange" key
    my_dict = asdict(model)
    del my_dict['exchange']
    model_dict = my_dict

    try:
        # Create a new token and add it to the database
        token = tables.NfoTokenTable(**model_dict) if model.exchange == 'NFO' else tables.NseTokenTable(**model_dict)
        db.add(token)
        db.commit()
        db.refresh(token)

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
            else:
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

    finally:
        # Close the database connection
        db.close()


def get_tokens(nse: bool) -> list:
    """Fetch token numbers for instruments NSE, NFO.

    Args:
        nse: If True, fetch token numbers for NSE instruments.

    Returns:
        A list of token numbers updated today.
    """

    # Get the token table name.
    token_table_name = tables.NseTokenTable if nse else tables.NfoTokenTable

    # Query the database for token numbers.
    tokens_data = token_db.query(token_table_name).filter(token_table_name.last_update == today)

    # Convert the query results to a list of token numbers.
    result = [item.instrument_token for item in tokens_data]

    # Return the list of token numbers.
    return result


def create_table_for_instruments(nse: bool = False):

    # Parameters
    tokens = get_tokens(nse=True) if nse else get_tokens(nse=False)
    engine = nse_tick_engine if nse else nfo_tick_engine

    for token in tokens:
        # Check if the table already exists
        with engine.connect() as conn:
            inspector = Inspector.from_engine(conn)
            if inspector.has_table(f"token_{token}"):
                # print(f'Table for token {token} already exists')
                return

        if nse:
            # Define the table for the NSE token
            class NseInstrumentTable(Base):
                __tablename__ = f"token_{token}"

                time_stamp = Column(DateTime, primary_key=True)
                price = Column(Float)
                average_price = Column(Float)
                total_buy_qty = Column(Integer)
                total_sell_qty = Column(Integer)
                traded_volume = Column(Integer)

            # Create the table in the database
            Base.metadata.create_all(engine, tables=[NseInstrumentTable.__table__])
            print(f'Table for token {token} created')

        else:
            # Define the table for the NFO token
            class NfoInstrumentTable(Base):
                __tablename__ = f"token_{token}"

                time_stamp = Column(DateTime, primary_key=True)
                price = Column(Float)
                average_price = Column(Float)
                total_buy_qty = Column(Integer)
                total_sell_qty = Column(Integer)
                traded_volume = Column(Integer)
                open_interest = Column(Integer)

            # Create the table in the database
            Base.metadata.create_all(engine, tables=[NfoInstrumentTable.__table__])
            print(f'Table for token {token} created')


decision = False
while decision is False:

    # Create a model to fetch data for NSE symbols
    nse_model = predefined_requests("token_NSE_ALL")
    # Add list of NSE stocks to name
    nse_model.name = tuple(activeSymbols.get_symbol_data(predefined_requests("stocks_NFO")))
    nse_tokens = activeSymbols.get_symbol_data(nse_model)

    # Fetch data for Futures NIFTY and BANKNIFTY
    futures_model = predefined_requests('futures_NI_BN')
    futures_tokens = activeSymbols.get_symbol_data(futures_model)

    # Fetch data for Options for BANKNIFTY
    options_model = predefined_requests('options_BN')
    # Take input for BANKNIFTY spot pre-open price
    options_model.strike = int(input("Enter BANKNIFTY spot pre-open price example: 43_600: "))
    options_tokens = activeSymbols.get_symbol_data(options_model)

    # Merge all tokens into a single list
    tokens_combined = nse_tokens + futures_tokens + options_tokens

    # Print the number of symbols for Futures, Options, and Stocks
    print(f"Futures:{len(futures_tokens)}\nOptions:{len(options_tokens)}\nStocks:{len(nse_tokens)}")

    decision = True if input("Press 'Y' to continue or 'N' to redo: ").upper() == 'Y' else False

    if decision:
        # add the tokens to the database
        for token_item in tokens_combined:
            add_token(token_item)

# Create missing tables of NSE
create_table_for_instruments(nse=True)

# Create missing tables of NFO
create_table_for_instruments(nse=False)

