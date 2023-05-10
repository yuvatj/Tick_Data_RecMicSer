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
from active_symbols import ActiveSymbols
from token_app.preset import predefined_requests
from database import Base, TokensSessionLocal,NseTickSessionLocal, nse_tick_engine, nfo_tick_engine


# Parameters
ut = Utility()
activeSymbols = ActiveSymbols()
today = datetime.today().date()


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
    tokens = nse_tokens + futures_tokens + options_tokens

    # Print the number of symbols for Futures, Options, and Stocks
    print(f"Futures:{len(futures_tokens)}\nOptions:{len(options_tokens)}\nStocks:{len(nse_tokens)}")

    decision = True if input("Press 'Y' to continue or 'N' to redo: ").upper() == 'Y' else False

    if decision:
        # add the tokens to the database
        for token in tokens:
            add_token(token)


def create_table_for_nfo_instrument(token):
    # Check if the table already exists
    with nfo_tick_engine.connect() as conn:
        inspector = Inspector.from_engine(conn)
        if inspector.has_table(f"token_{token}"):
            # print(f'Table for token {token} already exists')
            return

    # Define the table for the token
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
    Base.metadata.create_all(nfo_tick_engine, tables=[NfoInstrumentTable.__table__])
    print(f'Table for token {token} created')


def create_table_for_nse_instrument(token):
    # Check if the table already exists
    with nse_tick_engine.connect() as conn:
        inspector = Inspector.from_engine(conn)
        if inspector.has_table(f"token_{token}"):
            # print(f'Table for token {token} already exists')
            return

    # Define the table for the token
    class NseInstrumentTable(Base):
        __tablename__ = f"token_{token}"

        time_stamp = Column(DateTime, primary_key=True)
        price = Column(Float)
        average_price = Column(Float)
        total_buy_qty = Column(Integer)
        total_sell_qty = Column(Integer)
        traded_volume = Column(Integer)

    # Create the table in the database
    Base.metadata.create_all(nse_tick_engine, tables=[NseInstrumentTable.__table__])
    print(f'Table for token {token} created')


db = TokensSessionLocal()
tokens_data = db.query(tables.NfoTokenTable).filter(tables.NfoTokenTable.last_update == today)
tokens = [token.instrument_token for token in tokens_data]
for token in tokens:
    create_table_for_nfo_instrument(token)


db = TokensSessionLocal()
tokens_data = db.query(tables.NseTokenTable).filter(tables.NseTokenTable.last_update == today)
tokens = [token.instrument_token for token in tokens_data]
for token in tokens:
    create_table_for_nse_instrument(token)


# def add_nse_tick(model):
#     """ Function to add a token data to the database"""
#
#     # Connect to the database
#     db = NseTickSessionLocal()
#
#     # Create table name
#     table_name = f"token_{model.instrument_token}"
#
#     # Convert the model to a dictionary
#     my_dict = asdict(model)
#     del my_dict['instrument_token']
#     model_dict = my_dict
#
#     class NseTable(Base):
#         __tablename__ = table_name
#
#         time_stamp = Column(DateTime, primary_key=True)
#         price = Column(Float)
#         average_price = Column(Float)
#         total_buy_qty = Column(Integer)
#         total_sell_qty = Column(Integer)
#         traded_volume = Column(Integer)
#
#     table = NseTable(**model_dict)
#
#     try:
#         # Create a new token and add it to the database
#         token = table
#         db.add(token)
#         db.commit()
#
#     except IntegrityError as e:
#         # If there is an IntegrityError, rollback the transaction and handle the duplicate entry error
#         db.rollback()
#         if 'Duplicate entry' in str(e):
#             # If the model's exchange is NFO, update the existing NfoTable object
#             existing_token = db.query(tables.NseInstrumentTable)\
#                 .filter(NseTable.time_stamp == model.time_stamp).first()
#
#             # Update the values of the existing token
#             existing_token.price = model.price
#             existing_token.average_price = model.average_price
#             existing_token.total_buy_qty = model.total_buy_qty
#             existing_token.total_sell_qty = model.total_sell_qty
#             existing_token.traded_volume = model.traded_volume
#
#             # Add the updated token to the database
#             db.add(existing_token)
#             db.commit()
#
#     finally:
#         # Close the database connection
#         db.close()
#
#
# nse_model = models.NseInstrumentModel(
#     instrument_token=103425,
#     price=59.6,
#     average_price=60.3,
#     traded_volume=560589,
#     total_buy_qty=56942,
#     total_sell_qty=569845,
#     time_stamp=datetime.now()
# )
#
# add_nse_tick(nse_model)