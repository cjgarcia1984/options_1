import yfinance as yf
import sqlite3
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import pandas as pd

# from src.broker_api import SchwabAPI  # Assuming you have a broker API wrapper


class ContractSelector(ABC):
    def __init__(
        self,
        db_name="data/options_data.db",
        live=False,
        min_data_points=10,
        use_open=False,
    ):
        """
        Base contract selector class.

        :param db_name: Path to SQLite database for historical data.
        :param live: If True, fetches live data; otherwise, uses historical data.
        :param min_data_points: Minimum required data points for historical contract selection.
        :param use_open: If True, uses same-day `Open` price; otherwise, uses previous day's `Close` for backtesting.
        """
        self.db_name = db_name
        self.live = live
        self.min_data_points = min_data_points
        self.use_open = use_open  # Determines whether to use Open or Close price
        if live:
            self.api = SchwabAPI()  # Live API client

    def _connect_db(self):
        """Creates a connection to the SQLite database."""
        return sqlite3.connect(self.db_name)

    @abstractmethod
    def get_available_contracts(self, ticker, reference_date=None):
        """Retrieve available contracts (either from historical DB or live API)."""
        pass

    def select_contract(self, ticker, reference_date=None, max_results=3):
        """
        Selects the most liquid and actively traded ATM contracts.

        :param ticker: Stock ticker symbol.
        :param reference_date: Date of contract selection.
        :param max_results: Maximum number of contracts to return.
        :return: List of dictionaries with contract details.
        """
        if not self.live and reference_date is None:
            reference_date = self._get_first_contract_date(ticker)
            if reference_date is None:
                return []  # No historical data available

        contracts = self.get_available_contracts(ticker, reference_date)

        if not contracts:
            return []  # No contracts available

        #  Get the stock price based on execution rules
        stock_price = (
            self._get_historical_spot_price(ticker, reference_date, self.use_open)
            if not self.live
            else self.api.get_spot_price(ticker)
        )

        if stock_price is None:
            print(
                f"Warning: No stock price available for {ticker} on {reference_date}. Skipping contract selection."
            )
            return []

        all_contracts = []

        for strike in contracts.keys():
            for expiration_date, contract_data in contracts[strike].items():
                if "call" in contract_data and "put" in contract_data:
                    call_data = contract_data["call"]
                    put_data = contract_data["put"]

                    #  Convert to numeric safely using `pd.to_numeric(errors="coerce")`
                    call_volume = pd.to_numeric(
                        call_data.get("volume", 0), errors="coerce"
                    )
                    put_volume = pd.to_numeric(
                        put_data.get("volume", 0), errors="coerce"
                    )
                    call_oi = pd.to_numeric(
                        call_data.get("open_interest", 0), errors="coerce"
                    )
                    put_oi = pd.to_numeric(
                        put_data.get("open_interest", 0), errors="coerce"
                    )
                    call_iv = pd.to_numeric(call_data.get("iv", 0.0), errors="coerce")
                    put_iv = pd.to_numeric(put_data.get("iv", 0.0), errors="coerce")

                    #  Replace NaN values with defaults
                    call_volume = 0 if pd.isna(call_volume) else int(call_volume)
                    put_volume = 0 if pd.isna(put_volume) else int(put_volume)
                    call_oi = 0 if pd.isna(call_oi) else int(call_oi)
                    put_oi = 0 if pd.isna(put_oi) else int(put_oi)
                    call_iv = 0.0 if pd.isna(call_iv) else float(call_iv)
                    put_iv = 0.0 if pd.isna(put_iv) else float(put_iv)

                    #  Use minimum liquidity between the call and put
                    min_liquidity = min(call_volume, put_volume, call_oi, put_oi)

                    all_contracts.append(
                        {
                            "strike": strike,
                            "expiration_date": expiration_date,
                            "reference_date": reference_date,
                            "stock_price": stock_price,
                            "liquidity_score": min_liquidity,  #  Minimum liquidity
                            "implied_volatility": (call_iv + put_iv)
                            / 2,  #  Average IV
                        }
                    )

        sorted_contracts = sorted(
            all_contracts,
            key=lambda x: (
                abs(x["strike"] - stock_price),
                -x["liquidity_score"],
                -x["implied_volatility"],
            ),
        )

        #  Return the top max_results contracts
        return sorted_contracts[:max_results]

    def _get_historical_spot_price(self, ticker, trade_date, use_open):
        """
        Fetches the stock's historical price using Yahoo Finance.
        - Uses previous day's `Close` if `use_open=False` (for market open trading).
        - Uses same-day `Open` if `use_open=True` (for intraday analysis).
        - Finds the closest available price if data is missing due to weekends/holidays.

        :param ticker: Stock ticker symbol (e.g., "AAPL").
        :param trade_date: Date for which the stock price is needed (YYYY-MM-DD).
        :param use_open: If True, returns the `Open` price of the given date instead of `Close` from the previous day.
        :return: Stock price on the given date (or closest available date before it).
        """
        try:
            trade_date_obj = datetime.strptime(trade_date, "%Y-%m-%d")

            # Fetch stock history for a small window around the trade date
            stock = yf.Ticker(ticker)
            df = stock.history(
                start=trade_date_obj - timedelta(days=5),
                end=trade_date_obj + timedelta(days=1),
            )

            if df.empty:
                print(f"No stock price found for {ticker} near {trade_date}.")
                return None

            # Convert index to naive datetime for comparison
            df.index = df.index.tz_localize(None)

            if use_open:
                df = df[df.index == trade_date_obj]  # Get exact open price
                if not df.empty:
                    return df["Open"].iloc[0]
            else:
                df = df[
                    df.index < trade_date_obj
                ]  # Get last available close price before trade date
                if not df.empty:
                    return df["Close"].iloc[-1]

            print(f"No valid stock price found for {ticker} on {trade_date}.")
            return None

        except Exception as e:
            print(f"Error fetching stock price for {ticker} on {trade_date}: {e}")
            return None

    def _get_first_contract_date(self, ticker):
        """Finds the first available trade date for a given ticker."""
        conn = self._connect_db()
        result = conn.execute(
            """
            SELECT MIN(lastTradeDate) FROM options WHERE ticker=?
            """,
            (ticker,),
        ).fetchone()
        conn.close()
        return result[0] if result else None