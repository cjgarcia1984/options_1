import sqlite3
import pandas as pd
from datetime import datetime


class DataProvider:
    def __init__(self, db_name="data/options_data.db", live=False):
        self.db_name = db_name
        self.live = live
        if live:
            self.api = SchwabAPI()  # Instantiate broker API client

    def _connect_db(self):
        return sqlite3.connect(self.db_name)

    def load_contract(
        self, ticker, option_type, expiration_date, strike, interval="1T"
    ):
        """
        Loads option contract data, either from the database (historical) or live API.
        """
        if self.live:
            return self._load_live_contract(
                ticker, option_type, expiration_date, strike, interval
            )
        else:
            return self._load_historical_contract(
                ticker, option_type, expiration_date, strike, interval
            )

    def _load_historical_contract(
        self, ticker, option_type, expiration_date, strike, interval
    ):
        """
        Loads historical option contract data from SQLite database and resamples to specified interval.
        """
        conn = self._connect_db()
        query = """
            SELECT lastTradeDate AS Date, lastPrice AS Close, bid, ask, volume, openInterest, impliedVolatility,
                   percentChange, change, inTheMoney
            FROM options
            WHERE ticker=? AND option_type=? AND expiration_date=? AND strike=?
        """
        df = pd.read_sql_query(
            query,
            conn,
            params=[ticker, option_type, expiration_date, strike],
            parse_dates=["Date"],
        )
        conn.close()

        if df.empty:
            print(
                f"No historical data found for {ticker} {option_type} {strike} exp {expiration_date}"
            )
            return None

        df.set_index("Date", inplace=True)
        df.sort_index(inplace=True)

        # Construct OHLCV for backtesting
        df["Open"] = df[["bid", "ask"]].mean(axis=1).fillna(df["Close"])
        df["High"] = df[["Close", "bid", "ask"]].max(axis=1)
        df["Low"] = df[["Close", "bid", "ask"]].min(axis=1)

        df_resampled = (
            df[
                [
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "volume",
                    "openInterest",
                    "impliedVolatility",
                    "percentChange",
                    "change",
                    "inTheMoney",
                ]
            ]
            .resample(interval)
            .agg(
                {
                    "Open": "first",
                    "High": "max",
                    "Low": "min",
                    "Close": "last",
                    "volume": "sum",
                    "openInterest": "sum",
                    "impliedVolatility": "mean",
                    "percentChange": "mean",
                    "change": "mean",
                    "inTheMoney": "last",
                }
            )
            .ffill()
        )

        return df_resampled

    def _load_live_contract(
        self, ticker, option_type, expiration_date, strike, interval
    ):
        """
        Fetches real-time option chain data from the broker API and formats it for backtesting.
        """
        live_data = self.api.get_option_price(
            ticker, option_type, expiration_date, strike
        )

        df = pd.DataFrame(live_data)
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        df.sort_index(inplace=True)

        df["Open"] = df["Close"]  # In live mode, Open=Close if no bid/ask available
        df["High"] = df["Close"]
        df["Low"] = df["Close"]

        df_resampled = (
            df.resample(interval)
            .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
            .ffill()
        )

        return df_resampled

    def create_data(self, contracts, reference_date=None):
        """
        Combines multiple contracts into a single DataFrame for backtesting.
        
        :param contracts: List of contracts (each containing ticker, option_type, expiration_date, strike).
        :param reference_date: The date from which to start the backtest.
        :return: Processed DataFrame.
        """
        dfs = [self.load_contract(**contract) for contract in contracts]
        dfs = [df for df in dfs if df is not None]  # Filter out any None values
    
        if not dfs:
            print("No valid contract data available.")
            return None
    
        combined_df = pd.concat(dfs, axis=0).sort_index()  # Use `pd.concat()` instead of `sum()`
        combined_df.dropna(inplace=True)
    
        # Ensure we only test from the reference date onward
        if reference_date:
            combined_df = combined_df.loc[combined_df.index >= reference_date]
    
        return combined_df
    