from backtesting import Backtest
from src.data_provider import DataProvider  # Import the new abstraction


class BacktestEngine:
    def __init__(self, db_name="data/options_data.db", live=False):
        self.data_provider = DataProvider(db_name, live=live)

    def run_backtest(self, strategy, contracts, cash=10000, commission=0.001):
        # Load contracts using DataProvider
        bt_data = strategy.create_data(self.data_provider, contracts)
        bt = Backtest(bt_data, strategy, cash=cash, commission=commission)
        result = bt.run()
        bt.plot(superimpose=False)
        return result
