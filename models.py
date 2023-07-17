# Python Standard Library
from typing import Tuple
from datetime import date, datetime


# Data classes
from dataclasses import dataclass, field


# Local Library imports


# Parameters
today = datetime.today().date()


@dataclass()
class CredentialsModel:
    api_key: str
    api_secret: str
    totp_secret: str
    kite_username: str
    kite_password: str
    mysql_username: str
    mysql_password: str
    access_token: str = field(default=None)


@dataclass
class SymbolsModel:
    name: Tuple[str, ...] | str
    columns: Tuple[str, ...] | str
    exchange: str = field(default=None)
    segment: str = field(default=None)
    strike: int = field(default=None)
    instrument_type: str = field(default=None)
    expiry: str = field(default=None)
    strike_multiplier: int = field(default=None)
    strike_range_per_side: int = field(default=None)


@dataclass
class NfoTokenModel:
    instrument_token: int = field(default=None)
    tradingsymbol: str = field(default=None)
    lot_size: int = field(default=None)
    name: str = field(default=None)
    expiry: date = field(default=None)
    strike: int = field(default=None)
    segment: str = field(default=None)
    instrument_type: int = field(default=None)
    last_update: date = field(default=today)
    exchange: str = field(default='NFO')


@dataclass
class NseTokenModel:
    instrument_token: int = field(default=None)
    tradingsymbol: str = field(default=None)
    name: str = field(default=None)
    bank_nifty: float = field(default=0.0)
    nifty: float = field(default=0.0)
    fin_nifty: float = field(default=0.0)
    last_update: date = field(default=today)
    exchange: str = field(default='NSE')


@dataclass
class IndexTokenModel:
    instrument_token: int = field(default=None)
    tradingsymbol: str = field(default=None)
    name: str = field(default=None)
    last_update: date = field(default=today)
    exchange: str = field(default='INDEX')

