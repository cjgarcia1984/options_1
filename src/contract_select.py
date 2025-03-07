from abc import ABC, abstractmethod
import sqlite3
import pandas as pd

# Contract Selection Base Class
class ContractSelector(ABC):
    def __init__(self, db_name="data/options_data.db"):
        self.db_name = db_name

    def _connect_db(self):
        return sqlite3.connect(self.db_name)

    @abstractmethod
    def select_contract(self, ticker):
        pass

class StraddleSelector(ContractSelector):
    def __init__(self, db_name="data/options_data.db", min_data_points=10):
        super().__init__(db_name)
        self.min_data_points = min_data_points

    def select_contract(self, ticker):
        conn = self._connect_db()
        selected_contract = {}

        spot_price = conn.execute(
            """
            SELECT lastPrice FROM options 
            WHERE ticker=? ORDER BY lastTradeDate DESC LIMIT 1
            """,
            (ticker,),
        ).fetchone()[0]

        query = """
            SELECT strike, expiration_date
            FROM options
            WHERE ticker = ?
            GROUP BY strike, expiration_date
            HAVING
                SUM(CASE WHEN option_type='call' THEN 1 ELSE 0 END) >= ? AND
                SUM(CASE WHEN option_type='put' THEN 1 ELSE 0 END) >= ?
            ORDER BY ABS(strike - ?) ASC, expiration_date ASC
        """

        pairs = conn.execute(
            query, (ticker, self.min_data_points, self.min_data_points, spot_price)
        ).fetchall()

        for strike, expiration_date in pairs:
            call_dates = pd.read_sql_query(
                """
                SELECT lastTradeDate FROM options 
                WHERE ticker=? AND strike=? AND expiration_date=? AND option_type='call'
                """,
                conn,
                params=[ticker, strike, expiration_date],
            )

            put_dates = pd.read_sql_query(
                """
                SELECT lastTradeDate FROM options 
                WHERE ticker=? AND strike=? AND expiration_date=? AND option_type='put'
                """,
                conn,
                params=[ticker, strike, expiration_date],
            )

            overlap_dates = set(call_dates["lastTradeDate"]).intersection(
                set(put_dates["lastTradeDate"])
            )

            if len(overlap_dates) >= self.min_data_points:
                selected_contract = {
                    "strike": strike,
                    "expiration_date": expiration_date,
                }
                break

        if not selected_contract:
            print(
                f"No valid paired contracts with sufficient data found for ticker {ticker}"
            )

        conn.close()
        return selected_contract