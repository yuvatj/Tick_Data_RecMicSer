from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base


# Local Library imports
import utility
from kite_login import LoginCredentials

# Parameters
ut = utility.Utility()
kite_log = LoginCredentials()
log = kite_log.credentials


# Configuration for the tokens database
TOKENS_DATABASE_URL = f"mysql+pymysql://{log.mysql_username}:{log.mysql_password}@127.0.0.1:3306/tokens"
tokens_engine = create_engine(TOKENS_DATABASE_URL)
TokensSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=tokens_engine)


# Configuration for the NSE database
NSE_DATABASE_URL = f"mysql+pymysql://{log.mysql_username}:{log.mysql_password}@127.0.0.1:3306/nse_tick_data"
nse_tick_engine = create_engine(NSE_DATABASE_URL)
NseTickSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=nse_tick_engine)


# Configuration for the NFO database
NFO_DATABASE_URL = f"mysql+pymysql://{log.mysql_username}:{log.mysql_password}@127.0.0.1:3306/nfo_tick_data"
nfo_tick_engine = create_engine(NFO_DATABASE_URL)
NfoTickSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=nfo_tick_engine)


# Configuration for the Base
Base = declarative_base()