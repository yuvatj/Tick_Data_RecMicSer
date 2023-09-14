# Python Standard Library
import os
import json
import pandas as pd
from kiteconnect import KiteConnect
from datetime import datetime, timedelta

# Local Library imports
import models
from utility import Utility
from kite_login import LoginCredentials

# # Permanently changes the pandas settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Parameters
user = LoginCredentials()
log = user.credentials
ut = Utility()


class BaseSymbols:

    def __init__(self, exchange: str):

        # Check for valid exchange input
        if exchange not in ['NSE', 'NFO']:
            raise ValueError("Invalid exchange. Valid options are 'NSE' or 'NFO'.")

        # Class Parameters
        self.exchange = exchange
        self.date = datetime.today().date()
        self.nse_file = f"InstrumentToken/NSE/{self.date}_NSE.csv"
        self.nfo_file = f"InstrumentToken/NFO/{self.date}_NFO.csv"
        self.index_derivatives = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']

        # Class Base data for data_frame
        self.nse_csv_data = None
        self.nfo_csv_data = None

        self.nse_data_frame = None
        self.nfo_data_frame = None

        # Initiate some functions to load data
        self.__fetch_data_from_folder("NSE")  # order is important
        self.__fetch_data_from_folder("NFO")  # order is important

        self.__fetch_data_frame('NSE')
        self.__fetch_data_frame('NFO')

        # Check the validity of the data frame
        if self.nse_data_frame is None or self.nse_data_frame.empty \
                and self.nfo_data_frame is None or self.nfo_data_frame.empty:
            raise ValueError('Invalid data frame')

        # self.indices = self.__get_indices()
        self.nfo_stocks_list = self.__get_nfo_stocks_list()

    def __download_csv(self, exchange=None):
        """ Downloads the 'Instruments' detail file from kite API, according to input 'NSE' 'NFO' """

        data = None
        attempts = 0
        max_attempts = 3
        folder = "InstrumentToken"
        exchange = self.exchange if exchange is None else exchange

        file = f"{self.date}_{exchange}.csv"

        # Initialize KiteConnect with API key and access token
        kite = KiteConnect(api_key=log.api_key)
        kite.set_access_token(log.access_token)

        while data is None and attempts < max_attempts:
            try:
                # Request instruments data from Kite API for the specified exchange
                dump = kite.instruments(exchange)
                data_frame = pd.DataFrame(dump)

                # Drop rows with NaN values
                data_frame = data_frame.dropna()

                # If the data_frame has data after dropping NaN values, set 'data' to the processed DataFrame
                if not data_frame.empty:
                    data = data_frame

            except Exception as error:
                # If there is an error in downloading the file, print the error message
                print(f"Error in downloading {exchange} file from kite. Error: {error}")
                attempts += 1

        # Create folders to store the downloaded CSV file
        os.makedirs(folder, exist_ok=True)
        os.makedirs(f"{folder}/{exchange}", exist_ok=True)

        # Empty the folder before storing the new CSV file
        ut.empty_the_folder(f"{folder}/{exchange}")

        # Save the downloaded data to a CSV file
        data.to_csv(f"{folder}/{exchange}/{file}", index=False)

        print(f"{file}: downloaded")

    def __fetch_data_from_folder(self, exchange=None):
        data = None
        attempts = 0
        max_attempts = 3
        exchange = self.exchange if exchange is None else exchange

        while data is None and attempts < max_attempts:
            try:
                # Try to open csv file according to input
                file = self.nse_file if exchange == 'NSE' else self.nfo_file
                data = pd.read_csv(file)

            except FileNotFoundError:
                # Download the required CSV file
                self.__download_csv(exchange)

                attempts += 1

        if data is None:
            raise FileNotFoundError(f"Failed to fetch {exchange} data after multiple attempts.")

        if exchange == 'NSE':
            self.nse_csv_data = data
        elif exchange == 'NFO':
            self.nfo_csv_data = data

    def __fetch_data_frame(self, exchange=None):

        exchange = self.exchange if exchange is None else exchange
        data_frame = self.nse_csv_data if exchange == "NSE" else self.nfo_csv_data

        # Set the columns that need to be removed from data_frame based on the exchange
        drop_columns_nfo = ["exchange_token", "exchange", "tick_size", "last_price"]
        drop_columns_nse = ["exchange_token", "exchange", "last_price", "expiry", "strike", "tick_size",
                            "instrument_type"]

        drop_columns = drop_columns_nse if exchange == 'NSE' else drop_columns_nfo

        # Drop the columns from data_frame
        data_frame = data_frame.drop(columns=drop_columns)

        # Update the data_frame attribute if it's None, otherwise just return the processed data_frame
        if exchange == "NSE":

            # Drop rows with NaN values
            data_frame = data_frame.dropna()

            self.nse_data_frame = data_frame

        elif exchange == "NFO":
            self.nfo_data_frame = data_frame

    def __get_nfo_stocks_list(self):
        """
        Get a list of unique NFO stocks (Futures) excluding index derivatives.

        This method filters the data_frame to include only rows where the 'instrument_type' column
        has the value 'FUT' representing Futures. Then, it retrieves the 'name' column from the filtered
        data_frame and creates a set to remove duplicates. Finally, it removes specific elements (index
        derivatives) from the set to get the final list of unique NFO stocks.

        Returns:
            list: A list of unique NFO stocks excluding index derivatives.
        """
        # Get data_frame from the Base class
        data_frame = self.nfo_data_frame

        # Filter the data_frame to include only rows where the 'instrument_type' column has the value 'FUT'
        condition = data_frame['instrument_type'] == "FUT"
        data_frame = data_frame[condition]

        # Get a list of unique 'name' values (NFO stocks)
        stocks_list = list(data_frame['name'])

        # Create a set to remove duplicates and then convert it back to a list
        unique_names = list(set(stocks_list))

        # Remove specific elements (index derivatives) from the list
        names_list = [name for name in unique_names if name not in self.index_derivatives]

        # Sort the list in ascending order
        names_list.sort()

        return names_list


class NfoActiveSymbols(BaseSymbols):
    def __init__(self):
        super().__init__('NFO')

        # Class Parameters
        self.ltp_NIFTY = None
        self.ltp_FINNIFTY = None
        self.ltp_BANKNIFTY = None

        # Initiate some functions to load data
        self.__fetch_ltp()  # order is important

        self.futures_Nifty = self.__get_futures('NIFTY')
        self.futures_FINNIFTY = self.__get_futures('FINNIFTY')
        self.futures_BANKNIFTY = self.__get_futures('BANKNIFTY')

        self.options_Nifty = self.__get_options('NIFTY')
        self.options_FINNIFTY = self.__get_options('FINNIFTY')
        self.options_BANKNIFTY = self.__get_options('BANKNIFTY')

    def __fetch_ltp(self):

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

        ltp_nifty = index_quotes['NSE:NIFTY 50']['last_price']
        ltp_banknifty = index_quotes['NSE:NIFTY BANK']['last_price']
        ltp_finnifty = index_quotes['NSE:NIFTY FIN SERVICE']['last_price']

        self.ltp_NIFTY = round(ltp_nifty / 100) * 100
        # self.ltp_BANKNIFTY = round(ltp_banknifty / 100) * 100
        self.ltp_FINNIFTY = round(ltp_finnifty / 100) * 100

        self.ltp_BANKNIFTY = 46000

    def __get_futures(self, selection: str):
        # Define the trigger limit (number of days before the expiry date to trigger an action)
        trigger_limit = 3

        data_frame = self.nfo_data_frame.copy()  # Create a copy to avoid modifying the original data frame

        # Filter the DataFrame to include only rows with the specified 'name' and 'segment'
        data_frame = data_frame[(data_frame['name'] == selection) & (data_frame['segment'] == 'NFO-FUT')]

        # Convert 'expiry' column to datetime type and keep only the date part
        data_frame.loc[:, 'expiry'] = pd.to_datetime(data_frame['expiry']).dt.date

        # Sort DataFrame based on 'expiry' column in ascending order
        data_frame = data_frame.sort_values(by='expiry')

        # Get the expiry date of the first row (the nearest expiry date)
        expiry_date = data_frame.iloc[0]['expiry']

        # Calculate the trigger date (3 days before the nearest expiry date)
        trigger_date = expiry_date - timedelta(days=trigger_limit)

        # Get the current date
        current_date = self.date

        # Check if the current date is in the last week of the month (on or after the trigger date)
        last_week = (current_date >= trigger_date)

        # Convert DataFrame rows to StockData data class objects and store them in a list
        nfo_models = [models.NfoTokenModel(**row.to_dict()) for _, row in data_frame.iterrows()]

        if last_week:
            # Return the first two rows if it's the last week
            return nfo_models[:2]
        else:
            # Return the first row and the trigger date if it's not the last week
            return [nfo_models[0]]

    def __get_options(self, selection: str):

        atm_strike = None
        per_side_strikes = None

        if selection == 'NIFTY':
            atm_strike = self.ltp_NIFTY
            per_side_strikes = 5
        elif selection == 'BANKNIFTY':
            atm_strike = self.ltp_BANKNIFTY
            per_side_strikes = 12
        elif selection == 'FINNIFTY':
            atm_strike = self.ltp_FINNIFTY
            per_side_strikes = 5

        strike_multiplier = 100

        upper_limit = atm_strike + (per_side_strikes * strike_multiplier)
        lower_limit = atm_strike - (per_side_strikes * strike_multiplier)

        data_frame = self.nfo_data_frame.copy()  # Create a copy to avoid modifying the original data frame

        # Convert 'expiry' column to datetime type and keep only the date part
        data_frame['expiry'] = pd.to_datetime(data_frame['expiry']).dt.date

        # Filter the DataFrame to include only rows with the specified 'name' and 'segment'
        data_frame = data_frame[(data_frame['name'] == selection) & (data_frame['segment'] == 'NFO-OPT')]

        # Filter the DataFrame to include only rows for the nearest expiry date and strike divisible by 100
        data_frame = data_frame[
            (data_frame['expiry'] == data_frame['expiry'].min()) & (data_frame['strike'] % 100 == 0)]

        # Filter the DataFrame to include only rows between lower_limit and upper_limit in the "strike" column
        data_frame = data_frame[(data_frame['strike'] >= lower_limit) & (data_frame['strike'] <= upper_limit)]

        # Sort the data frame in descending order based on the 'strike' column
        data_frame.sort_values(by='strike', ascending=False, inplace=True)

        # Calculate the distance from the neutral strike and convert it to integer type
        data_frame['position'] = ((data_frame['strike'] - atm_strike) / strike_multiplier).astype(int)

        # # Convert DataFrame rows to StockData data class objects and store them in a list
        # nfo_models = [models.NfoTokenModel(**row.to_dict()) for _, row in data_frame.iterrows()]

        data_frame = data_frame[data_frame['instrument_type'] == 'PE']

        return data_frame # nfo_models

    def to_tokens(self):

        # futures = self.futures_Nifty + self.futures_BANKNIFTY + self.futures_FINNIFTY
        # options = self.options_Nifty + self.options_BANKNIFTY + self.options_FINNIFTY

        futures = self.futures_Nifty + self.futures_BANKNIFTY + self.futures_FINNIFTY
        options = self.options_Nifty + self.options_BANKNIFTY + self.options_FINNIFTY

        print(f"futures {len(futures)} downloaded")
        print(f"options {len(options)} downloaded")

        combined_tokens = futures + options

        # Remove duplicates while maintaining the order using list comprehension
        unique_tokens = [token for index, token in enumerate(combined_tokens) if token not in combined_tokens[:index]]

        return unique_tokens


class NseActiveSymbols(BaseSymbols):
    def __init__(self):
        super().__init__('NSE')

        self.data_frame = None

        # Initiate some functions to load data
        self.__fetch_nse_data_frame()

        self.stocks_nfo = self.__get_stocks('NFO')
        self.stocks_nse = self.__get_stocks('NSE')

        self.stocks_nifty = self.__get_stocks('NIFTY')
        self.stocks_finnifty = self.__get_stocks('FINNIFTY')
        self.stocks_banknifty = self.__get_stocks('BANKNIFTY')

    def __fetch_nse_data_frame(self):
        """
        Fetch NSE data frame and update it with NFO and index symbols information.

        This method fetches the NSE data frame and updates it with NFO and index symbols information.
        It sets the "NFO" column to 1 if the "tradingsymbol" is in the nfo_stocks_list, otherwise set it to 0.
        For each index symbol, it loads the data from the corresponding JSON file and sets the column with the
        values for the matching "tradingsymbol," otherwise, it sets the column value to 0.

        Raises:
            FileNotFoundError: If any index symbol JSON file is not found.
            ValueError: If there are missing items in any index symbol JSON file.

        """
        data_frame = self.nse_data_frame
        nfo_stocks_list = self.nfo_stocks_list
        index_instruments = self.index_derivatives

        # Create a new column "NFO" and set 1 if "tradingsymbol" is in the list, otherwise set 0
        data_frame["NFO"] = data_frame["tradingsymbol"].apply(lambda x: 1 if x in nfo_stocks_list else 0)

        for index_symbol in index_instruments:
            # Load data from the index symbol JSON file
            try:
                with open(f"{index_symbol}.json", "r") as json_file:
                    data = json.load(json_file)
            except FileNotFoundError as e:
                raise FileNotFoundError(f"File not found at the {index_symbol}.json location") from e

            stock_list = list(data.keys())

            # Check for missing symbols in the JSON data
            missing_symbols = [item for item in stock_list if item not in nfo_stocks_list]

            if missing_symbols:
                raise ValueError(f"Missing items from {index_symbol}: {missing_symbols}")

            # Create a new column with the index symbol as column name and set values based on matching "tradingsymbol"
            data_frame[index_symbol] = data_frame["tradingsymbol"].apply(lambda x: data[x] if x in stock_list else 0)

        # Update the main data_frame with the processed data
        self.data_frame = data_frame

    def __get_stocks(self, selection: str):

        data_frame = self.data_frame.copy()

        # Filter the data_frame to include only rows where the 'segment' column has the value 'NSE'
        condition = data_frame['segment'] == "NSE"

        if selection == 'NFO':
            condition = data_frame['NFO'] > 0

        elif selection == 'NIFTY':
            condition = data_frame['NIFTY'] > 0

        elif selection == 'BANKNIFTY':
            condition = data_frame['BANKNIFTY'] > 0

        elif selection == 'FINNIFTY':
            condition = data_frame['FINNIFTY'] > 0

        data_frame = data_frame[condition]

        # Drop the columns from data_frame in-place
        drop_columns = ["lot_size", "segment", "NFO"]
        data_frame = data_frame.drop(columns=drop_columns)

        # Rename the columns
        data_frame = data_frame.rename(columns={
            "NIFTY": "nifty",
            "BANKNIFTY": "bank_nifty",
            "FINNIFTY": "fin_nifty"
        })

        # Convert DataFrame rows to StockData data class objects and store them in a list
        nse_models = [models.NseTokenModel(**row.to_dict()) for _, row in data_frame.iterrows()]

        # Return the count of stocks in the filtered data_frame
        return nse_models

    def to_tokens(self):

        combined_tokens = self.stocks_nifty + self.stocks_banknifty + self.stocks_finnifty

        # Remove duplicates while maintaining the order using list comprehension
        unique_tokens = [token for index, token in enumerate(combined_tokens) if token not in combined_tokens[:index]]

        print(f"stocks {len(unique_tokens)} downloaded")

        return unique_tokens


class IndexActiveSymbols(BaseSymbols):
    def __init__(self):
        super().__init__('NSE')

        self.data_frame = None
        self.index_tokens = None
        self.index_list = ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE']
        # self.index_list = ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE', 'NIFTY MID SELECT']

        # Initiate some functions to load data
        self.__fetch_dataframe()
        self.__fetch_tokens()

    def __fetch_dataframe(self):

        data_frame = self.nse_data_frame.copy()

        data_frame = data_frame[data_frame['segment'] == 'INDICES']

        condition = ['instrument_token', 'tradingsymbol', 'name']

        self.data_frame = data_frame[condition]

    def __fetch_tokens(self):

        data_frame = self.data_frame
        model_list = []

        # Convert DataFrame rows to StockData data class objects and store them in a list
        index_models = [models.IndexTokenModel(**row.to_dict()) for _, row in data_frame.iterrows()
                        if row['tradingsymbol'] in self.index_list]

        self.index_tokens = index_models

    def to_tokens(self):

        combined_tokens = self.index_tokens

        # Remove duplicates while maintaining the order using list comprehension
        unique_tokens = [token for index, token in enumerate(combined_tokens) if token not in combined_tokens[:index]]

        print(f"Index derivatives {len(unique_tokens)} downloaded")

        return unique_tokens


if __name__ == "__main__":

    # nse_tokens = NseActiveSymbols()
    # print(nse_tokens.to_tokens())

    nfo_tokens = NfoActiveSymbols()
    # print(nfo_tokens.to_tokens())

    print(nfo_tokens.options_BANKNIFTY)

    # index_tokens = IndexActiveSymbols()
    # print(index_tokens.to_tokens())