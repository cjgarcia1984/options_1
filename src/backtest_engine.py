from backtesting import Backtest
from src.data_provider import DataProvider  # Import the new abstraction
import numpy as np


class BacktestEngine:
    def __init__(self, db_name="data/options_data.db", live=False):
        self.data_provider = DataProvider(db_name, live=live)

    def run_backtest(
        self, strategy, contracts, reference_date, cash=10000, commission=0.001
    ):
        """
        Runs a backtest and logs additional statistics.
        """
        bt_data = self.data_provider.create_data(contracts, reference_date)
        if bt_data is None:
            print("Skipping backtest due to missing data.")
            return None

        bt = Backtest(bt_data, strategy, cash=cash, commission=commission)
        result = bt.run()

        # Retrieve the actual strategy instance used during backtest
        strategy_instance = result._strategy

        # Extract trade results and store entry/exit reasons
        trades = result._trades.copy()

        if not trades.empty:
            trades["entry_reason"] = strategy_instance.entry_reason
            trades["exit_reason"] = strategy_instance.exit_reason

        # Compute additional stats safely
        total_profit = trades["PnL"].sum() if not trades.empty else 0.0
        num_trades = len(trades)
        win_rate = (trades["PnL"] > 0).mean() * 100 if not trades.empty else 0.0
        max_drawdown = result.get("Max. Drawdown [%]", 0.0)  # Avoid missing values
        sharpe_ratio = result.get("Sharpe Ratio", 0.0)  # Avoid NaN values

        # Store in dictionary
        summary_stats = {
            "total_profit": total_profit,
            "win_rate": win_rate,  # Now correctly defaults to 0.0
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "num_trades": num_trades,
        }

        return {
            "contracts": contracts,
            "results": trades if not trades.empty else None,
            "summary": summary_stats,
        }
