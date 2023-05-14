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
from database import TokensSessionLocal

# Parameters
log = LoginCredentials().credentials
today = datetime.today().date()
logging.basicConfig(level=logging.DEBUG)


class TickData:

    def __init__(self):
        self.today = today
        self.nse_tokens = self.__get_tokens(nse=True)
        self.nfo_tokens = self.__get_tokens(nse=False)

        self.nse_column = self.__get_column(nse=True)
        self.nfo_column = self.__get_column(nse=False)

        self.nse_path = "NSE_ticks"
        self.nfo_path = "NFO_ticks"

    @staticmethod
    def __get_column(nse=False):

        column_dict = {'time_stamp': ('datetime primary key', 'exchange_timestamp'),
                       'price': ('real(15,5)', 'last_price'),
                       'average_price': ('real(15,5)', 'average_traded_price'),
                       'total_buy_qty': ('integer', 'total_buy_quantity'),
                       'total_sell_qty': ('integer', 'total_sell_quantity'),
                       'traded_volume': ('integer', 'volume_traded')}

        if not nse:
            nfo_column_dict = column_dict
            nfo_column_dict.update({'open_interest': ('integer', 'oi')})

            return nfo_column_dict

        return column_dict

    def __get_tokens(self, nse: bool) -> list:
        """Fetch token numbers for instruments NSE, NFO.

        Args:
            nse: If True, fetch token numbers for NSE instruments.

        Returns:
            A list of token numbers.
        """

        # Get a database session.
        db = TokensSessionLocal()

        # Get the token table name.
        token_table_name = tables.NseTokenTable if nse else tables.NfoTokenTable

        # Query the database for token numbers.
        tokens_data = db.query(token_table_name).filter(token_table_name.last_update == self.today)

        # Convert the query results to a list of token numbers.
        tokens = [item.instrument_token for item in tokens_data]

        # Return the list of token numbers.
        return tokens

    @staticmethod
    def tcp_connection(_tokens: list, function: object, end: str):

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

        # Define a function to handle errors that occur when connecting to the websocket
        # or when subscribing to instruments.
        def handle_error(ws, error):
            # Log the error message.
            logging.error(error)

            # Close the websocket connection.
            ws.close()

        # Define a function to log the time when the websocket connects and disconnects,
        # as well as the list of instruments that are subscribed to.
        def log_connection_info(ws, event):
            # Log the time when the websocket connects or disconnects.
            logging.info(f"{event} at {datetime.now()}")

            # If the event is `connect`, log the list of instruments that are subscribed to.
            if event == "connect":
                logging.info(f"Subscribed to {_tokens}")

        # Assign the callbacks to the corresponding methods on the KiteTicker class.
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_error = handle_error
        kws.on_log = log_connection_info

        # Start the websocket connection and subscribe to the list of instruments.
        kws.connect(threaded=True)
        print('recording')

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
        print(f"program stopped at {current_time}")

    def record(self, exchange: str, token_list: list, column_dict: dict):

        # Initialise some parameters
        selection = exchange.upper()
        end_time_str = f"{self.today} 15:31:00"

        folder = self.nse_path if selection == "NSE" else self.nfo_path
        path = folder + f"/{today}.db"

        os.makedirs(folder, exist_ok=True)

        # Initiate the SQL server.
        server = Sqlite3Server(path, column_dict)

        # Fetch the tokens from the list of symbols.
        tokens = token_list

        # Create the tables in the SQLite database.
        server.create_tables(tokens)

        # Establish a TCP connection with the API.
        self.tcp_connection(tokens, server.insert_ticks, end_time_str)


if __name__ == '__main__':

    tick = TickData()
    tick.record('NFO', tick.nfo_tokens, tick.nfo_column)

