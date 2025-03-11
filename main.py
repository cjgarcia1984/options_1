import yaml
import pandas as pd
from src.backtest_engine import BacktestEngine
from src.contract_select import StraddleSelector
from src.strategy import SimpleStraddleStrategy

# Load config.yaml
with open("configs/backtest_config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Extract settings from YAML
db_path = config["data"]["db_path"]
results_csv = config["data"]["results_csv"]
reference_date = config["backtest"]["reference_date"]
max_contracts_per_ticker = config["backtest"]["max_contracts_per_ticker"]
tickers = config["backtest"]["tickers"]

# Initialize backtesting engine and contract selector
engine = BacktestEngine(db_path)
selector = StraddleSelector(db_path, use_open=True)

all_results = []

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
        result = engine.run_backtest(SimpleStraddleStrategy, contracts, reference_date=reference_date)

        if result:
            for trade in result["results"].to_dict(orient="records"):
                trade["ticker"] = ticker
                trade["strike"] = contract["strike"]
                trade["expiration_date"] = contract["expiration_date"]
                trade["option_type"] = "straddle"
                trade["entry_reason"] = result["results"]["entry_reason"]
                trade["exit_reason"] = result["results"]["exit_reason"]
                all_results.append(trade)

# Convert results to DataFrame and save
if all_results:
    results_df = pd.DataFrame(all_results)
    results_df.to_csv(results_csv, index=False)
    print(f"Results saved to {results_csv}")
else:
    print("No valid results to save.")
