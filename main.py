from src.backtest_engine import BacktestEngine
from src.contract_select import StraddleSelector
from src.strategy import SimpleStraddleStrategy

# Updated Usage Example
if __name__ == "__main__":
    engine = BacktestEngine("data/options_data.db")
    selector = StraddleSelector("data/options_data.db")

    tickers = ["AAPL", "MSFT"]
    results = {}

    for ticker in tickers:
        selected_contract = selector.select_contract(ticker)
        if not selected_contract:
            print(f"No suitable contract found for {ticker}")
            results[ticker] = None
        else:
            contracts = [
                {
                    'ticker': ticker,
                    'option_type': 'call',
                    'expiration_date': selected_contract['expiration_date'],
                    'strike': selected_contract['strike']
                },
                {
                    'ticker': ticker,
                    'option_type': 'put',
                    'expiration_date': selected_contract['expiration_date'],
                    'strike': selected_contract['strike']
                }
            ]

            result = engine.run_backtest(SimpleStraddleStrategy, contracts)

            print(result)
            results[ticker] = result

    print(results)