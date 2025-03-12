import yaml
import pandas as pd
from src.backtest_engine import BacktestEngine
from src.straddle_selector import StraddleSelector
from src.strategy import SimpleStraddleStrategy
import os

# Load config.yaml
with open("configs/backtest_config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Extract settings
db_path = config["data"]["db_path"]
results_csv = config["data"]["results_csv"]
summary_csv = results_csv.replace(".csv", "_summary.csv")  # Save summary separately
reference_date = config["backtest"]["reference_date"]
max_contracts_per_ticker = config["backtest"]["max_contracts_per_ticker"]
tickers = config["backtest"]["tickers"]

# Initialize backtesting engine
engine = BacktestEngine(db_path)
selector = StraddleSelector(db_path, use_open=True)

all_results = []
all_summaries = []

for ticker in tickers:
    selected_contracts = selector.select_contract(
        ticker, reference_date=reference_date, max_results=max_contracts_per_ticker
    )

    if not selected_contracts:
        print(f"No suitable contracts found for {ticker}")
        continue

    for contract in selected_contracts:
        contracts = [
            {
                "ticker": ticker,
                "option_type": "call",
                "expiration_date": contract["expiration_date"],
                "strike": contract["strike"],
            },
            {
                "ticker": ticker,
                "option_type": "put",
                "expiration_date": contract["expiration_date"],
                "strike": contract["strike"],
            },
        ]

        print(
            f"Running backtest for {ticker} - Strike: {contract['strike']}, Expiration: {contract['expiration_date']}"
        )
        result = engine.run_backtest(
            SimpleStraddleStrategy, contracts, reference_date=reference_date
        )
        if result:
            if (
                result and result["results"] is not None
            ):  # ✅ Check if results exist before accessing
                for i, trade in enumerate(result["results"].to_dict(orient="records")):
                    trade["ticker"] = ticker
                    trade["strike"] = contract["strike"]
                    trade["expiration_date"] = contract["expiration_date"]
                    trade["option_type"] = "straddle"

                    # Assign the correct entry/exit reason per trade
                    trade["entry_reason"] = result["results"].iloc[i]["entry_reason"]
                    trade["exit_reason"] = result["results"].iloc[i]["exit_reason"]

                    all_results.append(trade)

                # Append summary stats
                summary = result["summary"]
                summary["ticker"] = ticker
                summary["strike"] = contract["strike"]
                summary["expiration_date"] = contract["expiration_date"]
                all_summaries.append(summary)
            else:
                print(
                    f"No trades executed for {ticker} - {contract['strike']} exp {contract['expiration_date']}. Skipping."
                )


# Save trade results
if all_results:
    results_df = pd.DataFrame(all_results)
    os.makedirs(os.path.dirname(results_csv), exist_ok=True)
    results_df.to_csv(results_csv, index=False)
    print(f"Results saved to {results_csv}")
else:
    print("No valid results to save.")

# Save summary stats
if all_summaries:
    summary_df = pd.DataFrame(all_summaries)
    summary_df.to_csv(summary_csv, index=False)
    print(f"Summary stats saved to {summary_csv}")


