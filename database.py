from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base


# Local Library imports
import utility
from kite_login import LoginCredentials

# Parameters
ut = utility.Utility()
login = LoginCredentials()
log = login.credentials


# Configuration for the tokens database
DATABASE_URL_TOKENS = f"mysql+pymysql://{log.mysql_username}:{log.mysql_password}@127.0.0.1:3306/tokens"
engine_tokens = create_engine(DATABASE_URL_TOKENS)
SessionLocalTokens = sessionmaker(autocommit=False, autoflush=False, bind=engine_tokens)


# Configuration for the NSE database
DATABASE_URL_NSE = f"mysql+pymysql://{log.mysql_username}:{log.mysql_password}@127.0.0.1:3306/candle_data_nse"
engine_candle_data_nse = create_engine(DATABASE_URL_NSE)
SessionLocalCandleDataNse = sessionmaker(autocommit=False, autoflush=False, bind=engine_candle_data_nse)


# Configuration for the NFO database
DATABASE_URL_NFO = f"mysql+pymysql://{log.mysql_username}:{log.mysql_password}@127.0.0.1:3306/candle_data_nfo"
engine_candle_data_nfo = create_engine(DATABASE_URL_NFO)
SessionLocalCandleDataNfo = sessionmaker(autocommit=False, autoflush=False, bind=engine_candle_data_nfo)


# Configuration for the Index database
DATABASE_URL_INDEX = f"mysql+pymysql://{log.mysql_username}:{log.mysql_password}@127.0.0.1:3306/candle_data_index"
engine_candle_data_index = create_engine(DATABASE_URL_INDEX)
SessionLocalCandleDataIndex = sessionmaker(autocommit=False, autoflush=False, bind=engine_candle_data_index)


# Configuration for the Base
Base = declarative_base()