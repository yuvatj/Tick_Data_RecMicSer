# STD library
import os
import time
import logging
from datetime import datetime
from kiteconnect import KiteTicker

# local library import
import tables
from kite_login import LoginCredentials
from sqllite_local import Sqlite3Server
from database import SessionLocalTokens

# Parameters
log = LoginCredentials().credentials
today = datetime.today().date()
logging.basicConfig(level=logging.DEBUG)


class TickData:

    def __init__(self):
        self.today = today
        self.nse_tokens = self.__get_tokens('NSE')
        self.nfo_tokens = self.__get_tokens('NFO')
        self.index_tokens = self.__get_tokens('INDEX')

        self.nse_column = self.__get_column('NSE')
        self.nfo_column = self.__get_column('NFO')
        self.index_column = self.__get_column('INDEX')

        self.nse_path = 'E:/Market Analysis/Programs/Deployed/utility/Tick_data/NSE'
        self.nfo_path = 'E:/Market Analysis/Programs/Deployed/utility/Tick_data/NFO'

        # self.nse_path = "NSE_ticks"
        # self.nfo_path = "NFO_ticks"

        self.index_path = "INDEX_ticks"

    @staticmethod
    def __get_column(exchange: str):

        exchange = exchange.upper()
        column_dict = None

        if exchange == 'NSE':

            column_dict = {'time_stamp': ('datetime primary key', 'exchange_timestamp'),
                           'price': ('real(15,5)', 'last_price'),
                           'average_price': ('real(15,5)', 'average_traded_price'),
                           'total_buy_qty': ('integer', 'total_buy_quantity'),
                           'total_sell_qty': ('integer', 'total_sell_quantity'),
                           'volume': ('integer', 'volume_traded')}

        elif exchange == 'NFO':

            column_dict = {'time_stamp': ('datetime primary key', 'exchange_timestamp'),
                           'price': ('real(15,5)', 'last_price'),
                           'average_price': ('real(15,5)', 'average_traded_price'),
                           'total_buy_qty': ('integer', 'total_buy_quantity'),
                           'total_sell_qty': ('integer', 'total_sell_quantity'),
                           'volume': ('integer', 'volume_traded'),
                           'open_interest': ('integer', 'oi')}

        elif exchange == 'INDEX':

            column_dict = {'time_stamp': ('datetime primary key', 'exchange_timestamp'),
                           'price': ('real(15,5)', 'last_price')}

        return column_dict

    def __get_tokens(self, exchange: str) -> list:
        """Fetch token numbers for instruments NSE, NFO.

        Args:
            exchange: 'NSE', fetch token numbers for NSE instruments.

        Returns:
            A list of token numbers.
        """

        # Parameters
        exchange = exchange.upper()
        table_model = None

        # Get a database session.
        db = SessionLocalTokens()

        # Get the token table.
        if exchange == 'NSE':
            table_model = tables.NseTokenTable
        elif exchange == 'NFO':
            table_model = tables.NfoTokenTable
        elif exchange == 'INDEX':
            table_model = tables.IndexTokenTable

        # Query the database for token numbers.
        tokens_data = db.query(table_model).filter(table_model.last_update == self.today)

        # Convert the query results to a list of token numbers.
        tokens_ = [item.instrument_token for item in tokens_data]

        # Return the list of token numbers.
        return tokens_

    @staticmethod
    def tcp_connection(_tokens: list, function: object, end: str, exchange: str):

        # Create a KiteTicker object with the api_key and access_token.
        kws = KiteTicker(log.api_key, log.access_token)

        # Convert the end date time string to a datetime object.
        end_time = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        # Define a callback function to be called when the websocket receives a tick message.
        def on_ticks(ws, ticks):
            # Call the function passed in to the tcp_connection() method with the tick data.
            function(ticks)

        # Define a callback function to be called when the websocket connects to the Kite Connect server.
        def on_connect(ws, response):
            # Subscribe to the list of instruments passed in to the tcp_connection() method.
            ws.subscribe(_tokens)

            # Set the mode for the list of instruments to `full`.
            ws.set_mode(ws.MODE_FULL, _tokens)

        # Assign the callbacks to the corresponding methods on the KiteTicker class.
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect

        # Start the websocket connection and subscribe to the list of instruments.
        kws.connect(threaded=True)
        print(f'{exchange}: recording started')

        # Infinite loop on the main thread. Nothing after this will run.
        while True:
            # Get the current time.
            current_time = datetime.now().time().replace(microsecond=0)

            # If the current time is greater than or equal to the end time,
            # close the websocket connection and break out of the loop.
            if current_time >= end_time.time():
                kws.close()
                break

            # Otherwise, sleep for the difference between the current time and the end time.
            else:
                time_diff = (end_time - datetime.today()).seconds
                time.sleep(time_diff + 1)

        # Print a message to indicate that the program has stopped.
        print(f"{exchange}: recording stopped at {current_time}")

    def record(self, exchange: str):

        # Initialise some parameters
        selection = exchange.upper()
        end_time_str = f"{self.today} 15:31:00"

        folder = None
        column_dict = None
        tokens_ = None

        if selection == 'NSE':
            folder = self.nse_path
            column_dict = self.nse_column
            tokens_ = self.nse_tokens

        elif selection == 'NFO':
            folder = self.nfo_path
            column_dict = self.nfo_column
            tokens_ = self.nfo_tokens

        elif selection == 'INDEX':
            folder = self.index_path
            column_dict = self.index_column
            tokens_ = self.index_tokens

        path = folder + f"/{today}.db"

        os.makedirs(folder, exist_ok=True)

        # Initiate the SQL server.
        server = Sqlite3Server(path, column_dict)

        # Create the tables in the SQLite database.
        server.create_tables(tokens_)

        # Establish a TCP connection with the API.
        self.tcp_connection(tokens_, server.insert_ticks, end_time_str, exchange)


if __name__ == '__main__':
    tick = TickData()
    tokens = [256265, 257801, 260105]
    date = "2023-06-01 23:43:00"
    tick.tcp_connection(tokens, print, date, 'INDEX')
