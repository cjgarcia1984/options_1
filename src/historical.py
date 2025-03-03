from src.data import DataHandler
import yfinance as yf
from datetime import datetime
import pandas as pd

class HistoricalDataHandler(DataHandler):
    def __init__(self, db_name='options_data.db', stock_list=None):
        super().__init__(db_name)
        self.stock_list = stock_list if stock_list else []

    def fetch_and_store_options_data(self):
        for ticker_symbol in self.stock_list:
            try:
                ticker = yf.Ticker(ticker_symbol)
                expiration_dates = ticker.options
                for exp_date in expiration_dates:
                    options = ticker.option_chain(exp_date)
                    calls = options.calls
                    puts = options.puts

                    # Add metadata to the DataFrame
                    calls['option_type'] = 'call'
                    puts['option_type'] = 'put'
                    calls['expiration_date'] = exp_date
                    puts['expiration_date'] = exp_date

                    # Combine calls and puts
                    options_data = pd.concat([calls, puts])

                    # Add current timestamp
                    options_data['retrieval_date'] = datetime.now()

                    options_data['ticker'] = ticker_symbol

                    # Insert data into the database
                    self.insert_data(options_data.astype(str))

                print(f"Options data for {ticker_symbol} fetched and stored successfully.")

            except Exception as e:
                print(f"An error occurred for {ticker_symbol}: {e}")