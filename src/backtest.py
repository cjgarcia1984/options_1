import sqlite3
import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import SMA


class BacktestEngine:
    def __init__(self, db_name='data/options_data.db'):
        self.db_name = db_name

    def _connect_db(self):
        """Establish a database connection."""
        return sqlite3.connect(self.db_name)

    def load_data(self, ticker, option_type=None, expiration_date=None, strike=None, start_date=None, end_date=None):
        """
        Load historical options data using lastTradeDate for accurate price timestamps,
        and format it for backtesting.
        """
        conn = self._connect_db()
    
        query = '''
        SELECT lastTradeDate, contractSymbol, lastPrice, bid, ask, volume, openInterest, 
               impliedVolatility, strike, expiration_date, option_type
        FROM options WHERE ticker = ?
        '''
        params = [ticker]
    
        if option_type:
            query += " AND option_type = ?"
            params.append(option_type)
    
        if expiration_date:
            query += " AND expiration_date = ?"
            params.append(expiration_date)
    
        if strike:
            query += " AND strike = ?"
            params.append(strike)
    
        if start_date:
            query += " AND lastTradeDate >= ?"
            params.append(start_date)
    
        if end_date:
            query += " AND lastTradeDate <= ?"
            params.append(end_date)
    
        # Use lastTradeDate as the date field
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['lastTradeDate'])
        conn.close()
    
        # Sort by the trade date
        df = df.sort_values(by="lastTradeDate")
    
        # Rename for backtesting: use lastTradeDate as Date and lastPrice as Close
        df = df.rename(columns={'lastTradeDate': 'Date', 'lastPrice': 'Close'})
        df = df[['Date', 'Close']].set_index('Date')
    
        # Create synthetic OHLC columns using Close
        df['Open'] = df['Close']
        df['High'] = df['Close']
        df['Low'] = df['Close']
    
        # Reorder columns to the required order
        df = df[['Open', 'High', 'Low', 'Close']]
    
        return df
    

    def run_backtest(self, strategy, ticker, start_date=None, end_date=None):
        """Run a backtest using a given strategy on the options data."""
        df = self.load_data(ticker, start_date=start_date, end_date=end_date)
        bt = Backtest(df, strategy, cash=10000, commission=0.002)
        results = bt.run()
        bt.plot()
        return results

    def select_contracts(self, ticker, option_type, min_volume=100, min_open_interest=100, delta_range=(-0.5, 0.5)):
        """
        Selects liquid options contracts based on volume, open interest, and delta.
        Note: The yfinance schema doesn't include 'delta' by default.
        If you want to use delta, ensure your data includes it or compute it.
        """
        conn = self._connect_db()

        query = '''
        SELECT contractSymbol, strike, expiration_date, volume, openInterest, 
               impliedVolatility, inTheMoney
        FROM options 
        WHERE ticker = ? AND option_type = ? AND volume >= ? AND openInterest >= ?
        '''
        params = [ticker, option_type, min_volume, min_open_interest]

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        # If 'delta' is available, filter by the delta range
        if 'delta' in df.columns:
            df = df[(df['delta'] >= delta_range[0]) & (df['delta'] <= delta_range[1])]

        return df

    def run_backtest_with_selection(self, strategy, ticker, option_type="CALL", start_date=None, end_date=None):
        """
        Runs a backtest using the best available options contract.
        Chooses the contract with the highest volume.
        """
        # Select the most liquid contract
        contracts = self.select_contracts(ticker, option_type)

        if contracts.empty:
            print(f"No suitable contracts found for {ticker}")
            return None

        # Pick the contract with the highest volume (assuming ATM or other criteria are handled externally)
        best_contract = contracts.iloc[contracts['volume'].idxmax()]

        print(
            f"Selected contract: {best_contract['contractSymbol']} (Strike: {best_contract['strike']}, Exp: {best_contract['expiration_date']})")

        # Load data for the chosen contract
        df = self.load_data(
            ticker=ticker,
            option_type=option_type,
            expiration_date=best_contract['expiration_date'],
            strike=best_contract['strike'],
            start_date=start_date,
            end_date=end_date
        )

        # Run the backtest
        bt = Backtest(df, strategy, cash=10000, commission=0.002)
        results = bt.run()
        bt.plot()
        return results


# Example Strategy using Simple Moving Averages
class SMAStrategy(Strategy):
    short_window = 10
    long_window = 50

    def init(self):
        self.sma_short = self.I(SMA, self.data.Close, self.short_window)
        self.sma_long = self.I(SMA, self.data.Close, self.long_window)

    def next(self):
        if crossover(self.sma_short, self.sma_long):
            self.buy()
        elif crossover(self.sma_long, self.sma_short):
            self.sell()


# Example usage
if __name__ == "__main__":
    engine = BacktestEngine()
    results = engine.run_backtest_with_selection(
        SMAStrategy, ticker="MSFT", option_type="call", start_date="2023-01-01", end_date="2026-01-01")
    print(results)
