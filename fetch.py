from src.historical import HistoricalDataHandler

if __name__ == "__main__":
    stock_list = ['AAPL', 'MSFT', 'GOOGL',"SPY","TSLA"]  # Add your desired tickers here
    data_handler = HistoricalDataHandler(stock_list=stock_list)
    data_handler.fetch_and_store_options_data()
    data_handler.export_to_csv('options_data_export.csv')
    data_handler.close_connection()