import json


# Local Library imports
import tables
import models
from utility import Utility
from active_symbols import ActiveSymbols


# Parameters
ut = Utility()
activeSymbols = ActiveSymbols()

# Banknifty is used to generate the expiry dates.
bn_current_month_expiry = activeSymbols.get_expiry_dates('BANKNIFTY')[0]
bn_current_week_expiry = activeSymbols.get_expiry_dates('BANKNIFTY', options=True)[0]


def predefined_requests(selection: str):

    model = None

    if selection == 'futures_NI_BN':
        model = models.SymbolsModel(
            name=('NIFTY', 'BANKNIFTY'),
            exchange='NFO',
            segment='NFO-FUT',
            columns=('tradingsymbol', 'instrument_token', 'lot_size', 'name', 'expiry',
                     'strike', 'segment', 'instrument_type')
        )

    elif selection == 'options_BN':
        model = models.SymbolsModel(
            name='BANKNIFTY',
            exchange='NFO',
            segment='NFO-OPT',
            strike=43_400,
            strike_multiplier=100,
            strike_range_per_side=15,
            columns=('tradingsymbol', 'instrument_token', 'lot_size', 'name', 'expiry',
                     'strike', 'segment', 'instrument_type'),
            expiry=bn_current_week_expiry
        )

    elif selection == 'stocks_NFO':
        model = models.SymbolsModel(
            name='ALL',
            exchange='NFO',
            segment='NFO-FUT',
            columns='name',
            expiry=bn_current_month_expiry
        )

    elif selection == 'token_NSE_ALL':
        model = models.SymbolsModel(
            name=('AARTIIND', 'ABB', 'ABBOTINDIA', 'ABCAPITAL'),
            exchange='NSE',
            columns=('tradingsymbol', 'instrument_token', 'name', 'lot_size')
        )

    elif selection == 'INDEX':
        model = models.SymbolsModel(
            name=('NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE'),
            exchange='INDEX',
            columns=('tradingsymbol', 'instrument_token', 'name')
        )

    return model


