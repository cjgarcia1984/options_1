import sqlite3
import pandas as pd
from backtesting import Backtest
from src.strategy import SimpleStraddleStrategy

# Updated BacktestEngine
class BacktestEngine:
    def __init__(self, db_name='data/options_data.db'):
        self.db_name = db_name

    def _connect_db(self):
        return sqlite3.connect(self.db_name)

    def load_contract(self, ticker, option_type, expiration_date, strike, interval='1T'):
        query = """
            SELECT lastTradeDate AS Date, lastPrice AS Close, bid, ask
            FROM options
            WHERE ticker=? AND option_type=? AND expiration_date=? AND strike=?
        """
        df = pd.read_sql_query(
            query, self._connect_db(),
            params=[ticker, option_type, expiration_date, strike],
            parse_dates=['Date']
        ).set_index('Date').sort_index()

        df['Open'] = df[['bid', 'ask']].mean(axis=1).fillna(df['Close'])
        df['High'] = df[['Close', 'bid', 'ask']].max(axis=1)
        df['Low'] = df[['Close', 'bid', 'ask']].min(axis=1)

        df_resampled = df[['Open', 'High', 'Low', 'Close']].resample(interval).agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
        }).ffill()

        return df_resampled

    def run_backtest(self, strategy, contracts, cash=10000, commission=0.001):
        # Pass individual contracts directly to strategy
        bt_data = strategy.create_data(self, contracts)
        bt = Backtest(bt_data, strategy, cash=cash, commission=commission)
        result = bt.run()
        bt.plot(superimpose=False)
        return result



# Example usage
if __name__ == "__main__":
    engine = BacktestEngine()

    contracts = [
        {'ticker': 'AAPL', 'option_type': 'call', 'expiration_date': '2025-03-21', 'strike': 150},
        {'ticker': 'AAPL', 'option_type': 'put', 'expiration_date': '2025-03-21', 'strike': 150}
    ]

    result = engine.run_backtest(contracts, SimpleHoldStrategy)

    print(result)
