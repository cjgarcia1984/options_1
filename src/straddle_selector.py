from src.contract_select import ContractSelector
import yfinance as yf
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


class StraddleSelector(ContractSelector):
    def get_available_contracts(self, ticker, reference_date=None):
        conn = self._connect_db()
        query = """
            SELECT strike, expiration_date, option_type, volume, openInterest, impliedVolatility
            FROM options
            WHERE ticker = ? AND lastTradeDate >= ?
        """
        rows = conn.execute(query, (ticker, reference_date)).fetchall()
        conn.close()

        contracts = {}

        for row in rows:
            strike, expiration_date, option_type, volume, open_interest, iv = row

            if volume in [None, "nan", "NaN"] or open_interest in [None, "nan", "NaN"]:
                continue

            try:
                volume = int(float(volume))
                open_interest = int(float(open_interest))
                iv = float(iv)
            except ValueError:
                continue

            if strike not in contracts:
                contracts[strike] = {}
            if expiration_date not in contracts[strike]:
                contracts[strike][expiration_date] = {}

            contracts[strike][expiration_date][option_type.lower()] = {
                "volume": volume,
                "open_interest": open_interest,
                "iv": iv,
            }

        return contracts

    def _compute_realized_volatility(self, ticker, days, reference_date):
        end_date = datetime.strptime(reference_date, "%Y-%m-%d")
        start_date = end_date - timedelta(days=days)
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        returns = hist["Close"].pct_change().dropna()
        realized_vol = returns.std() * np.sqrt(252)
        return realized_vol

    def select_contract(
        self,
        ticker,
        reference_date=None,
        max_results=3,
        historical_days=7,
        optimal_expiry_range=(7, 30),
    ):
        if not self.live and reference_date is None:
            reference_date = self._get_first_contract_date(ticker)
            if reference_date is None:
                return []

        contracts = self.get_available_contracts(ticker, reference_date)
        if not contracts:
            return []

        stock_price = (
            self._get_historical_spot_price(ticker, reference_date, self.use_open)
            if not self.live
            else self.api.get_spot_price(ticker)
        )

        if stock_price is None:
            print(f"No stock price available for {ticker} on {reference_date}.")
            return []

        hv = self._compute_realized_volatility(ticker, historical_days, reference_date)

        selected_contracts = []

        for strike, exp_data in contracts.items():
            for expiration_date, contract_data in exp_data.items():
                days_to_expiry = (
                    datetime.strptime(expiration_date, "%Y-%m-%d")
                    - datetime.strptime(reference_date, "%Y-%m-%d")
                ).days
                if (
                    not optimal_expiry_range[0]
                    <= days_to_expiry
                    <= optimal_expiry_range[1]
                ):
                    continue

                call_data = contract_data.get("call")
                put_data = contract_data.get("put")

                if not call_data or not put_data:
                    continue

                liquidity = min(
                    call_data["volume"],
                    put_data["volume"],
                    #call_data["open_interest"],
                    #put_data["open_interest"],
                )

                if liquidity == 0:
                    continue

                avg_iv = (call_data["iv"] + put_data["iv"]) / 2
                iv_hv_ratio = avg_iv / hv if hv != 0 else np.inf

                selected_contracts.append(
                    {
                        "strike": strike,
                        "expiration_date": expiration_date,
                        "days_to_expiry": days_to_expiry,
                        "reference_date": reference_date,
                        "stock_price": stock_price,
                        "liquidity": liquidity,
                        "iv_hv_ratio": iv_hv_ratio,
                        "strike_distance": abs(strike - stock_price),
                    }
                )

        sorted_contracts = sorted(
            selected_contracts,
            key=lambda x: (x["strike_distance"], x["iv_hv_ratio"], -x["liquidity"]),
        )

        return sorted_contracts[:max_results]
