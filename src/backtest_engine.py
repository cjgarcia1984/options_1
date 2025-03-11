from backtesting import Backtest
from src.data_provider import DataProvider  # Import the new abstraction


class BacktestEngine:
    def __init__(self, db_name="data/options_data.db", live=False):
        self.data_provider = DataProvider(db_name, live=live)

    def run_backtest(
        self, strategy, contracts, reference_date, cash=10000, commission=0.001
    ):
        """
        Runs a backtest and logs entry/exit reasons.
        """
        bt_data = self.data_provider.create_data(contracts, reference_date)
        if bt_data is None:
            print("Skipping backtest due to missing data.")
            return None

        bt = Backtest(bt_data, strategy, cash=cash, commission=commission)
        result = bt.run()

        # Retrieve the actual strategy instance used during backtest
        strategy_instance = result._strategy  # Correct way to get strategy instance

        # Extract trade results and store entry/exit reasons
        trades = result._trades.copy()
        trades["entry_reason"] = strategy_instance.entry_reason
        trades["exit_reason"] = strategy_instance.exit_reason

        return {"contracts": contracts, "results": trades}
