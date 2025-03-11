import yfinance as yf
import sqlite3
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

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
        Selects the best ATM contracts for the given ticker.

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

        # Get the stock price based on execution rules
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

        selected_contracts = []

        for strike in sorted(contracts.keys(), key=lambda s: abs(s - stock_price)):
            for expiration_date in sorted(contracts[strike].keys()):
                contract_data = contracts[strike][expiration_date]
                if "call" in contract_data and "put" in contract_data:
                    selected_contracts.append(
                        {
                            "strike": strike,
                            "expiration_date": expiration_date,
                            "reference_date": reference_date,
                            "stock_price": stock_price,
                        }
                    )

                    # Stop if we've reached the max number of results
                    if len(selected_contracts) >= max_results:
                        return selected_contracts

        return selected_contracts

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


class StraddleSelector(ContractSelector):
    def get_available_contracts(self, ticker, reference_date=None):
        """Fetches available contracts based on historical data (SQLite) or live data (API)."""
        if self.live:
            return self._get_live_contracts(ticker)
        else:
            return self._get_historical_contracts(ticker, reference_date)

    def _get_historical_contracts(self, ticker, reference_date):
        """
        Fetches historical option contracts that exist from the reference date onward.

        :param ticker: Stock ticker symbol.
        :param reference_date: The date when backtesting starts.
        :return: Dictionary with contract availability.
        """
        conn = self._connect_db()
        query = """
            SELECT strike, expiration_date, option_type, volume, openInterest, impliedVolatility
            FROM options
            WHERE ticker = ? AND lastTradeDate >= ?
            GROUP BY strike, expiration_date, option_type
            HAVING COUNT(*) >= ? AND volume > 0 AND openInterest > 0
        """
        rows = conn.execute(
            query, (ticker, reference_date, self.min_data_points)
        ).fetchall()
        conn.close()

        contracts = {}
        for strike, expiration_date, option_type, volume, open_interest, iv in rows:
            contracts.setdefault(strike, {}).setdefault(expiration_date, {})[
                option_type
            ] = {"volume": volume, "open_interest": open_interest, "iv": iv}

        return contracts

    def _get_live_contracts(self, ticker):
        """Fetches live option contracts from the broker API."""
        option_chain = self.api.get_option_chain(ticker)
        if not option_chain:
            return {}

        contracts = {}
        for strike, expirations in option_chain.items():
            for expiration_date, contract_data in expirations.items():
                contracts.setdefault(strike, {}).setdefault(expiration_date, {})
                if "call" in contract_data:
                    contracts[strike][expiration_date]["call"] = True
                if "put" in contract_data:
                    contracts[strike][expiration_date]["put"] = True

        return contracts
