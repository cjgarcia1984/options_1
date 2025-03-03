from src.historical import HistoricalDataHandler

if __name__ == "__main__":
    config_path = "/home/chris/options_1/config.yaml"  # Ensure the path is correct

    data_handler = HistoricalDataHandler(config_path=config_path)
    data_handler.fetch_and_store_options_data()
    data_handler.export_to_csv()
    data_handler.close_connection()
