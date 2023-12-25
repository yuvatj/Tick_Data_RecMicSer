# STD library
import os
import time
import pika
import json
from datetime import datetime
from kiteconnect import KiteTicker

# local library import
import tables
from kite_login import LoginCredentials
from sqllite_local import Sqlite3Server
from database import SessionLocalTokens

# Parameters
log = LoginCredentials()


class RabbitMQQueue:
    def __init__(self, exchange_name, queue_name, host='localhost', username='guest', password='guest'):
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.host = host
        self.credentials = pika.PlainCredentials(username, password)
        self.connection_params = pika.ConnectionParameters(host=self.host, credentials=self.credentials)
        self.connection = self.connect()

    def connect(self):
        try:
            connection = pika.BlockingConnection(self.connection_params)
            return connection

        except Exception as e:
            print(f"Error connecting to RabbitMQ: {e}")
            return None

    def declare_exchange(self):
        try:
            channel = self.connection.channel()
            channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
            print(f"Exchange '{self.exchange_name}' declared successfully")

        except Exception as e:
            print(f"Error declaring exchange: {e}")

    def declare_queue(self):
        try:
            channel = self.connection.channel()
            channel.queue_declare(queue=self.queue_name)
            print(f"Queue '{self.queue_name}' declared successfully")

        except Exception as e:
            print(f"Error declaring queue: {e}")

    def bind_queue_to_exchange(self):
        try:
            channel = self.connection.channel()
            channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.queue_name)
            print(f"Queue '{self.queue_name}' bound to exchange '{self.exchange_name}'")

        except Exception as e:
            print(f"Error binding queue to exchange: {e}")

    def publish_message(self, message):

        channel = self.connection.channel()
        channel.basic_publish(exchange=self.exchange_name, routing_key=self.queue_name, body=message)

        # try:
        #     channel = self.connection.channel()
        #     channel.basic_publish(exchange=self.exchange_name, routing_key=self.queue_name, body=message)
        #
        # except Exception as e:
        #     print(f"Error publishing message: {e}")

        # try:
        #     channel = self.connection.channel()
        #     channel.basic_publish(exchange=self.exchange_name, routing_key=self.queue_name, body=message)
        #     # print(f"Message sent to exchange '{self.exchange_name}' with routing key '{self.queue_name}': {message}")
        # except pika.exceptions.AMQPError as e:
        #     print(f"Error publishing message: {e}")

    def close_connection(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Connection to RabbitMQ closed")


class TickData:

    def __init__(self):
        self.today = datetime.today().date()
        self.nse_tokens = self.__get_tokens('NSE')
        self.nfo_tokens = self.__get_tokens('NFO')
        self.index_tokens = self.__get_tokens('INDEX')

        self.nse_column = self.__get_column('NSE')
        self.nfo_column = self.__get_column('NFO')
        self.index_column = self.__get_column('INDEX')

        self.nse_path = 'E:/Market Analysis/Programs/Deployed/utility/Tick_data/NSE'
        self.nfo_path = 'E:/Market Analysis/Programs/Deployed/utility/Tick_data/NFO'
        self.index_path = 'E:/Market Analysis/Programs/Deployed/utility/Tick_data/INDEX'

        # self.nse_txt_file = "nse_ticks_.txt"
        # self.nfo_txt_file = "nfo_ticks_.txt"
        # self.index_txt_file = 'index_ticks_.txt'

        self.nse_txt_file = f'E:/Market Analysis/Programs/Deployed/utility/Ticks_txt/NSE/{self.today}.txt'
        self.nfo_txt_file = f'E:/Market Analysis/Programs/Deployed/utility/Ticks_txt/NFO/{self.today}.txt'
        self.index_txt_file = f'E:/Market Analysis/Programs/Deployed/utility/Ticks_txt/INDEX/{self.today}.txt'

    # Custom JSON encoder to handle datetime objects
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.strftime('%Y-%m-%d %H:%M:%S')
            return super().default(obj)

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
        # tokens_data = db.query(table_model).filter(table_model.last_update == "2023-11-16")

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

    def tcp_connection_beta(self, _tokens: list, file_path: str, end: str, exchange: str):

        # Create a KiteTicker object with the api_key and access_token.
        kws = KiteTicker(log.api_key, log.access_token)

        # Convert the end date time string to a datetime object.
        end_time = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        message_broker = True

        try:
            # Implementation of rabbit_mq
            exchange_queue = RabbitMQQueue(exchange, f'{exchange}_queue')

            # Declare exchanges and queues
            exchange_queue.declare_exchange()
            exchange_queue.declare_queue()
            exchange_queue.bind_queue_to_exchange()

        except Exception as e:
            message_broker = False
            print(f"Error while connecting to rabbit_mq: {e}")

        # Open a text file for writing tick data
        with open(file_path, "a") as file:

            # Define a callback function to be called when the websocket receives a tick message.
            def on_ticks(ws, ticks):

                formatted_time = datetime.now().time().strftime("%H:%M:%S.%f")[:-3]
                final_data = {formatted_time: str(ticks)}

                # Method 01
                # Serialize the data to JSON and write to the file
                json.dump(final_data, file)

                # Write a newline character to separate each JSON object
                file.write('\n')

                # Insert message to rabbit_mq
                if message_broker:
                    try:
                        exchange_queue.publish_message(json.dumps(final_data))

                    except Exception as e:
                        print(f"Error while connecting to rabbit_mq gg: {e}")

                        print("Retrying to connect")

                        # Implementation of rabbit_mq
                        exchange_queue_retry = RabbitMQQueue(exchange, f'{exchange}_queue')

                        # Declare exchanges and queues
                        exchange_queue_retry.declare_exchange()
                        exchange_queue_retry.declare_queue()
                        exchange_queue_retry.bind_queue_to_exchange()

                        exchange_queue_retry.publish_message(json.dumps(final_data))

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

                    # Close connections
                    exchange_queue.close_connection()

                    # Close kite connection
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

        path = folder + f"/{self.today}.db"

        os.makedirs(folder, exist_ok=True)

        # Initiate the SQL server.
        server = Sqlite3Server(path, column_dict)

        # Create the tables in the SQLite database.
        server.create_tables(tokens_)

        # Establish a TCP connection with the API.
        self.tcp_connection(tokens_, server.insert_ticks, end_time_str, exchange)

    def record_beta(self, exchange: str):

        end_time_str = f"{self.today} 15:31:00"

        file_mapping = {
            "NSE": (self.nse_txt_file, self.nse_tokens),
            "NFO": (self.nfo_txt_file, self.nfo_tokens),
            "INDEX": (self.index_txt_file, self.index_tokens)
        }

        file_name = file_mapping.get(exchange)[0]
        tokens_ = file_mapping.get(exchange)[1]

        # Establish a TCP connection with the API.
        self.tcp_connection_beta(tokens_, file_name, end_time_str, exchange)


if __name__ == '__main__':
    tick = TickData()

    nse_tokens = tick.index_tokens


    def write_to_txt(data):
        start_time = time.time()  # Record the start time

        # Specify the file path where you want to write the data
        file_path = "data_output.txt"  # Replace this with your desired file path

        formatted_time = datetime.now().time().strftime("%H:%M:%S.%f")[:-3]

        final_data = {formatted_time: str(data)}

        # Open the file in write mode ('w')
        with open(file_path, 'a') as file:
            # json.dump(final_data, file)

            # Method 02
            # Serialize the data to JSON
            tick_json = json.dumps(final_data)

            # Write the JSON data to the file
            file.write(tick_json + "\n")

        end_time = time.time()  # Record the end time

        # Calculate the time taken in seconds
        time_taken = end_time - start_time

        print(f"Data written to '{file_path}' in {time_taken:.5f} seconds.")

    # tokens = [256265, 257801, 260105]
    # date = "2023-11-20 15:30:00"
    # tick.tcp_connection(tokens, write_to_txt, date, 'INDEX')

    # tokens = nse_tokens
    # date = "2023-12-18 09:30:00"
    # tick.tcp_connection(tokens, print, date, 'NFO')

    exchange = 'INDEX'
    summery_date = "2023-12-18"
    date_time = "2023-11-20 15:30:00"

    file_path = f'E:/Market Analysis/Programs/Deployed/utility/Ticks_txt/{exchange}/{summery_date}.txt'

    tick.tcp_connection_beta(nse_tokens, file_path, date_time, 'INDEX')

