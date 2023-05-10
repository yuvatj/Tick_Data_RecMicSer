# STD library
import os
import time
import logging
import pandas as pd
from datetime import datetime
from kiteconnect import KiteTicker, KiteConnect
from dataclasses import dataclass, asdict
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Column, Integer, DateTime, Float, Table
# local library import
import models
import tables
from utility import Utility
from kite_login import LoginCredentials
from database import Base, TokensSessionLocal, NseTickSessionLocal, nse_tick_engine, nfo_tick_engine

# Parameters
ut = Utility()
log = LoginCredentials()
credentials = log.credentials
logging.basicConfig(level=logging.DEBUG)
today = ut.today


class TickData:

    @staticmethod
    def tcp_connection(tokens: list, function: object, end_time: str):

        # Initialise
        kws = KiteTicker(credentials.api_key, credentials.access_token)
        end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

        def on_ticks(ws, ticks):
            # Callback to receive ticks.
            # insert_ticks(ticks)
            function(ticks)
            # print(ticks)

        def on_connect(ws, response):
            # Callback on successful connect.
            # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)

        # Assign the callbacks.
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect

        # Infinite loop on the main thread. Nothing after this will run.
        # You have to use the pre-defined callbacks to manage subscriptions.
        kws.connect(threaded=True)
        print('recording')

        while True:
            # Get the current time
            current_time = datetime.now().time().replace(microsecond=0)
            if current_time >= end_time.time():
                kws.close()
                break
            else:
                time_diff = (end_time - datetime.today()).seconds
                time.sleep(time_diff + 1)
        print(f"program stopped at {current_time}")

    @classmethod
    def record(cls, segment: str, token_list: list, column_dict: dict):

        # Initialise some parameters
        selection = segment.upper()
        end_time_str = f"{today} 15:31:00"
        end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
        path = file_manger.tick_data_folder + f"NSE/{today}.db"

        if selection == "NFO":
            path = file_manger.tick_data_folder + f"NFO/{today}.db"

        # initiate sql server
        server = Sqlite3Server(path, column_dict)

        # fetch tokens from symbols
        tokens = token_list

        # create table
        server.create_tables(tokens)

        # establish TCP connection with api
        cls.tcp_connection(tokens, server.insert_ticks, end_time)


if __name__ == '__main__':

    db = TokensSessionLocal()

    tokens_data = db.query(tables.NseTokenTable).filter(tables.NseTokenTable.last_update == today)
    tokens = [token.instrument_token for token in tokens_data]


    def add_nse_tick(model):
        """ Function to add a token data to the database"""

        # Connect to the database
        db = NseTickSessionLocal()

        # Create table name
        table_name = f"token_{model.instrument_token}"

        # Convert the model to a dictionary
        my_dict = asdict(model)
        del my_dict['instrument_token']
        model_dict = my_dict

        class NseTable(Base):
            __tablename__ = table_name

            time_stamp = Column(DateTime, primary_key=True)
            price = Column(Float)
            average_price = Column(Float)
            total_buy_qty = Column(Integer)
            total_sell_qty = Column(Integer)
            traded_volume = Column(Integer)

        # Define the table object with `extend_existing=True`
        table = Table(table_name, Base.metadata, extend_existing=True, autoload=True)

        # Create a new row with the values from the model
        row = table.insert().values(**model_dict)

        try:
            # Add the new row to the database
            db.execute(row)
            db.commit()

        except IntegrityError as e:
            # If there is an IntegrityError, rollback the transaction and handle the duplicate entry error
            db.rollback()
            if 'Duplicate entry' in str(e):
                # If the model's exchange is NFO, update the existing NfoTable object
                existing_token = db.query(tables.NseInstrumentTable) \
                    .filter(NseTable.time_stamp == model.time_stamp).first()

                # Update the values of the existing token
                existing_token.price = model.price
                existing_token.average_price = model.average_price
                existing_token.total_buy_qty = model.total_buy_qty
                existing_token.total_sell_qty = model.total_sell_qty
                existing_token.traded_volume = model.traded_volume

                # Add the updated token to the database
                db.add(existing_token)
                db.commit()

        finally:
            # Close the database connection
            db.close()


    def demo(ticks):
        for tick in ticks:
            stock = models.NseInstrumentModel(
                instrument_token=tick['instrument_token'],
                price=tick['last_price'],
                traded_volume=tick['volume_traded'],
                total_buy_qty=tick['total_buy_quantity'],
                total_sell_qty=tick['total_sell_quantity'],
                time_stamp=tick['exchange_timestamp'],
                average_price=tick['average_traded_price']
            )
            add_nse_tick(stock)


    TickData.tcp_connection(tokens, demo, '2023-05-10 11:21:00')






