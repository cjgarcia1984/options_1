import sqlite3
import pandas as pd
from datetime import datetime
#from src.broker_api import SchwabAPI  # Assuming you have a Schwab API wrapper

class DataProvider:
    def __init__(self, db_name="data/options_data.db", live=False):
        self.db_name = db_name
        self.live = live
        if live:
            self.api = SchwabAPI()  # Instantiate broker API client

    def _connect_db(self):
        return sqlite3.connect(self.db_name)

    def load_contract(self, ticker, option_type, expiration_date, strike, interval="1T"):
        if self.live:
            return self._load_live_contract(ticker, option_type, expiration_date, strike, interval)
        else:
            return self._load_historical_contract(ticker, option_type, expiration_date, strike, interval)

    def _load_historical_contract(self, ticker, option_type, expiration_date, strike, interval):
        conn = self._connect_db()
        query = """
            SELECT lastTradeDate AS Date, lastPrice AS Close, bid, ask
            FROM options
            WHERE ticker=? AND option_type=? AND expiration_date=? AND strike=?
        """
        df = pd.read_sql_query(query, conn, params=[ticker, option_type, expiration_date, strike], parse_dates=["Date"])
        conn.close()
        
        df.set_index("Date", inplace=True)
        df.sort_index(inplace=True)

        # Construct OHLCV for backtesting
        df["Open"] = df[["bid", "ask"]].mean(axis=1).fillna(df["Close"])
        df["High"] = df[["Close", "bid", "ask"]].max(axis=1)
        df["Low"] = df[["Close", "bid", "ask"]].min(axis=1)

        df_resampled = df[["Open", "High", "Low", "Close"]].resample(interval).agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last"}
        ).ffill()

        return df_resampled

    def _load_live_contract(self, ticker, option_type, expiration_date, strike, interval):
        # Fetch real-time option chain data
        live_data = self.api.get_option_price(ticker, option_type, expiration_date, strike)

        df = pd.DataFrame(live_data)
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        df.sort_index(inplace=True)

        df["Open"] = df["Close"]  # In live mode, Open=Close if no bid/ask available
        df["High"] = df["Close"]
        df["Low"] = df["Close"]

        df_resampled = df.resample(interval).agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last"}
        ).ffill()

        return df_resampled
