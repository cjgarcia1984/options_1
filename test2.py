import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# List of stock ticker symbols
stock_list = ['AAPL', 'MSFT', 'GOOGL']  # Add your desired tickers here

# Directory to save the CSV files
data_dir = 'options_data'
os.makedirs(data_dir, exist_ok=True)

# Function to fetch and append options data
def fetch_and_append_options_data(ticker_symbol):
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

            # Define the CSV file path
            csv_file = os.path.join(data_dir, f'{ticker_symbol}_options.csv')

            # Check if the CSV file exists
            if os.path.exists(csv_file):
                # Append to existing CSV
                options_data.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                # Create a new CSV with headers
                options_data.to_csv(csv_file, mode='w', header=True, index=False)

        print(f"Options data for {ticker_symbol} fetched and appended successfully.")

    except Exception as e:
        print(f"An error occurred for {ticker_symbol}: {e}")

# Iterate over each stock and fetch data
for stock in stock_list:
    fetch_and_append_options_data(stock)
