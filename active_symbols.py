# Python Standard Library
import os
import pandas as pd
from datetime import datetime
from kiteconnect import KiteConnect


# Local Library imports
import models
from utility import Utility
from kite_login import LoginCredentials


# # Permanently changes the pandas settings
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)

# Parameters
kite_log = LoginCredentials()
log = kite_log.credentials
ut = Utility()


class ActiveSymbols:
    def __init__(self):
        self.today = ut.today
        self.instruments_NSE = self.get_instrument_token_file(nse=True)
        self.instruments_NFO = self.get_instrument_token_file()

    def download_instrument_token_file(self, nse: bool = False, nfo: bool = False):
        """ Downloads the 'Instruments' detail file from kite API, according to input 'NSE' 'NFO' """
        data = None
        folder = "InstrumentToken"
        file = None
        exchange = None

        kite = KiteConnect(api_key=log.api_key)
        kite.set_access_token(log.access_token)

        while data is None:
            if nse:
                try:
                    dump = kite.instruments("NSE")
                    data_frame = pd.DataFrame(dump)
                    data_frame['count'] = [len(x) for x in data_frame['name']]
                    data_frame = data_frame[data_frame['segment'] == 'NSE']
                    data_frame = data_frame[data_frame['count'] > 0]
                    data_frame = data_frame[['instrument_token', 'tradingsymbol', 'name', 'lot_size']]

                    data = data_frame
                    exchange = "NSE"
                    file = f"{self.today}_NSE.csv"

                except Exception as error:
                    print(f"get_instrument_token_file NSE error: {error}")

            elif nfo:
                try:
                    dump = kite.instruments('NFO')
                    data_frame = pd.DataFrame(dump)
                    data_frame = data_frame[['instrument_token', 'tradingsymbol', 'name', 'expiry', 'strike',
                                             'lot_size', 'instrument_type', 'segment']]
                    data = data_frame
                    exchange = "NFO"
                    file = f"{self.today}_NFO.csv"

                except Exception as error:
                    print(f"get_instrument_token_file NFO error: {error}")

        os.makedirs(folder, exist_ok=True)
        os.makedirs(f"{folder}/{exchange}", exist_ok=True)
        ut.empty_the_folder(f"{folder}/{exchange}")
        data.to_csv(f"{folder}/{exchange}/{file}", index=False)
        print(f"{file}: downloaded")

    def get_instrument_token_file(self, nse: bool = False):
        data = None

        while data is None:
            try:
                file = f"InstrumentToken/NSE/{self.today}_NSE.csv" if nse \
                    else f"InstrumentToken/NFO/{self.today}_NFO.csv"
                data = pd.read_csv(file)

            except FileNotFoundError:
                if nse:
                    self.download_instrument_token_file(nse=True)
                else:
                    self.download_instrument_token_file(nfo=True)

        return data

    def get_expiry_dates(self, symbol: str, options: bool = False, data_frm=None):

        # create data_frame
        data_frame = data_frm if data_frm is not None else self.instruments_NFO

        # Query to filter data-frame by name and segment
        query = f"name == '{symbol}' and segment == 'NFO-OPT'" if options \
            else f"name == '{symbol}' and segment == 'NFO-FUT'"

        filtered_df = data_frame.query(query)

        expiry = filtered_df['expiry'].unique()
        expiry_dates = sorted([datetime.strptime(str(date_str), '%Y-%m-%d') for date_str in expiry])
        dates = dict([(index, str(item)[:10]) for index, item in enumerate(expiry_dates)])

        return dates

    def get_symbol_data(self, data: models.SymbolsModel):

        # Some variables initiation
        name = data.name
        exchange = data.exchange.upper()
        strike = data.strike
        segment = data.segment
        instrument_type = data.instrument_type
        expiry = data.expiry
        columns = data.columns
        strike_range_per_side = data.strike_range_per_side if segment == 'NFO-OPT' else None
        strike_multiplier = data.strike_multiplier if segment == 'NFO-OPT' else None
        query = ''

        # create data_frame
        data_frame = self.instruments_NFO if exchange == 'NFO' else self.instruments_NSE

        def query_con():

            if name == 'ALL':
                _query = f"segment == '{segment}' and expiry == '{expiry}'"
                return _query

            elif exchange == 'NSE':
                _query = f"tradingsymbol.isin({list(name)})" if type(name) is tuple else f"tradingsymbol == '{name}'"
                return _query

            else:
                _query = ""
                # name handling is done for one or more symbols
                _query += f"name.isin({list(name)})" if type(name) is tuple else f"name == '{name}'"

                # This filters the segment and add segment in query
                if segment is not None:
                    _query += f" and segment == 'NFO-FUT'" if segment == 'NFO-FUT' else f" and segment == 'NFO-OPT'"

                if expiry is not None:
                    _query += f" and expiry == '{expiry}'"

                if instrument_type is not None:
                    _query += f" and instrument_type == 'CE'" \
                        if instrument_type == 'CE' \
                        else f" and instrument_type == 'PE'"

                if strike is not None:
                    range_value = strike_multiplier * strike_range_per_side
                    high = strike + range_value
                    low = strike - range_value
                    _query += f" and strike.between({low}, {high})"

                return _query

        query = query_con()
        # print(query)
        filtered_df = data_frame.query(query)
        return_data_frame = filtered_df[list(data.columns)] \
            if type(data.columns) is tuple else filtered_df[data.columns]

        if name == 'ALL' and type(columns) is not tuple:
            return return_data_frame.to_list()

        elif exchange == 'NFO':
            tokens = return_data_frame.to_dict('index')

            data = [models.NfoTokenModel(**tokens[item]) for item in tokens]
            return data

        elif exchange == 'NSE':
            tokens = return_data_frame.to_dict('index')

            data = [models.NseTokenModel(**tokens[item]) for item in tokens]
            return data

        else:
            tokens = return_data_frame.to_dict('index')
            return tokens


if __name__ == "__main__":
    symb = ActiveSymbols()
    # print(symb.instruments_NFO.head(10))

    futures = models.SymbolsModel(
        name=('NIFTY', 'BANKNIFTY'),
        exchange='NFO',
        segment='NFO-FUT',
        columns=('tradingsymbol', 'instrument_token', 'lot_size', 'name', 'expiry',
                 'strike', 'segment', 'instrument_type'),
        expiry="2023-05-25"
    )

    print(symb.get_symbol_data(futures))


