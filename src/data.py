import sqlite3
import pandas as pd


class DataHandler:
    def __init__(self, db_name='options_data.db'):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self._create_table()


    def _create_table(self):
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contractSymbol TEXT,
            lastTradeDate TEXT,
            strike REAL,
            lastPrice REAL,
            bid REAL,
            ask REAL,
            change REAL,
            percentChange REAL,
            volume INTEGER,
            openInterest INTEGER,
            impliedVolatility REAL,
            inTheMoney BOOLEAN,
            contractSize TEXT,
            currency TEXT,
            option_type TEXT,
            expiration_date TEXT,
            retrieval_date TEXT
        )
        ''')
        self.conn.commit()

    def insert_data(self, data):
        for _, row in data.iterrows():
            self.cursor.execute('''
            INSERT INTO options (
                contractSymbol, lastTradeDate, strike, lastPrice, bid, ask, change,
                percentChange, volume, openInterest, impliedVolatility, inTheMoney,
                contractSize, currency, option_type, expiration_date, retrieval_date, ticker
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(contractSymbol, lastTradeDate) DO UPDATE SET
                strike=excluded.strike,
                lastPrice=excluded.lastPrice,
                bid=excluded.bid,
                ask=excluded.ask,
                change=excluded.change,
                percentChange=excluded.percentChange,
                volume=excluded.volume,
                openInterest=excluded.openInterest,
                impliedVolatility=excluded.impliedVolatility,
                inTheMoney=excluded.inTheMoney,
                contractSize=excluded.contractSize,
                currency=excluded.currency,
                option_type=excluded.option_type,
                expiration_date=excluded.expiration_date,
                retrieval_date=excluded.retrieval_date,
                ticker=excluded.ticker
            ''', (
                row['contractSymbol'], row['lastTradeDate'], row['strike'], row['lastPrice'],
                row['bid'], row['ask'], row['change'], row['percentChange'], row['volume'],
                row['openInterest'], row['impliedVolatility'], row['inTheMoney'],
                row['contractSize'], row['currency'], row['option_type'], row['expiration_date'],
                row['retrieval_date'], row['ticker']
            ))
        self.conn.commit()


    def export_to_csv(self, csv_file='options_data_export.csv'):
        query = 'SELECT * FROM options'
        df = pd.read_sql_query(query, self.conn)
        df.to_csv(csv_file, index=False)
        print(f"Data exported successfully to {csv_file}")

    def close_connection(self):
        self.conn.close()
