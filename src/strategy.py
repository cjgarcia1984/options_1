from backtesting import Strategy
import numpy as np
import talib


class SimpleStraddleStrategy(Strategy):
    # Default Parameters
    base_hold_period = 3  # Default hold period in days
    cooldown_period = 1  # Cooldown before repurchasing
    profit_target = 0.2  # 50% profit target
    stop_loss = -0.1  # 25% stop loss
    min_iv = 0.2  # Minimum implied volatility threshold
    max_iv = 0.7  # Avoid buying options with extreme IV
    base_size = 10  # Base position size

    def init(self):
        self.has_bought = False
        self.entry_price = None
        self.entry_time = None
        self.exit_time = None
        self.entry_reason = None
        self.exit_reason = None
        self.hold_period = self.base_hold_period  # Default hold period

        # Indicators
        self.rsi = self.I(talib.RSI, self.data.Close, timeperiod=14)
        self.atr = self.I(
            talib.ATR, self.data.High, self.data.Low, self.data.Close, timeperiod=14
        )
        self.upper_band, self.middle_band, self.lower_band = self.I(
            talib.BBANDS, self.data.Close, timeperiod=20
        )
        self.iv = self.data.impliedVolatility

    def should_buy(self):
        """
        Determines if we should enter a straddle position.
        - Uses Bollinger Bands, IV Rank, RSI & ATR.
        - Confirms with volume for liquidity.
        """
        reasons = []

        # ✅ Bollinger Bands for volatility squeeze
        if self.data.Close[-1] <= self.lower_band[-1]:
            reasons.append("Bollinger Band Squeeze")

        # ✅ IV Rank (Ensure IV is relatively low compared to history)
        iv_rank = (self.iv[-1] - np.min(self.iv[-20:])) / (
            np.max(self.iv[-20:]) - np.min(self.iv[-20:])
        )
        #if iv_rank < 0.3:
        #    reasons.append("IV Rank Low (Cheap Options)")
        if iv_rank > 0.8:
            return False, "IV Rank Too High (Expensive Options)"

        # ✅ Volume Confirmation (Avoid illiquid trades)
        avg_volume = np.mean(self.data.volume[-20:])
        if self.data.volume[-1] < 1.5 * avg_volume:
            return False, "Low Volume Confirmation"

        # ✅ RSI & ATR-based volatility confirmation
        if self.rsi[-1] < 30:
            reasons.append("RSI Oversold")
        if self.rsi[-1] > 70:
            reasons.append("RSI Overbought")
        if self.atr[-1] > self.atr[-2]:
            reasons.append("Increasing ATR (Volatility)")

        return (True, ", ".join(reasons)) if reasons else (False, "")

    def should_exit(self):
        """
        Determines if we should exit the position.
        - Uses trailing ATR-based stop loss & time-based exit.
        """
        reasons = []

        # ✅ Adjust Hold Period Based on ATR Expansion
        if self.atr[-1] > 1.5 * np.mean(self.atr[-10:]):
            self.hold_period = 5  # Extend to 5 days
        else:
            self.hold_period = self.base_hold_period  # Reset to default

        # ✅ Time-based exit
        if self.data.index[-1] >= self.entry_time + np.timedelta64(
            self.hold_period, "D"
        ):
            reasons.append(f"Hold Period Expired ({self.hold_period} days)")

        # ✅ ATR-based Trailing Stop
        trailing_stop = self.entry_price - (self.atr[-1] * 1.5)
        if self.data.Close[-1] < trailing_stop:
            reasons.append("ATR Trailing Stop Hit")

        # ✅ Dynamic Profit Target
        price_change = (self.data.Close[-1] - self.entry_price) / self.entry_price
        if price_change >= self.profit_target:
            reasons.append(f"Profit Target Hit (+{self.profit_target * 100}%)")

        return (True, ", ".join(reasons)) if reasons else (False, "")

    def next(self):
        """
        Runs every tick to decide whether to buy or sell.
        """
        current_time = self.data.index[-1]

        # Cooldown: Prevent immediate repurchasing
        if self.exit_time and current_time < self.exit_time + np.timedelta64(
            self.cooldown_period, "D"
        ):
            return  # Skip buying if still in cooldown period

        if not self.has_bought:
            should_buy, reason = self.should_buy()
            if should_buy:
                self.entry_price = self.data.Close[-1]
                self.entry_time = current_time
                self.entry_reason = reason

                # ✅ Fix: Handle NaN ATR values before calculating position size
                current_atr = (
                    self.atr[-1]
                    if not np.isnan(self.atr[-1])
                    else np.nanmean(self.atr[-20:])
                )
                avg_atr = np.nanmean(
                    self.atr[-20:]
                )  # Use `nanmean` to avoid NaN issues

                # ✅ Ensure ATR is non-zero before division
                if np.isnan(current_atr) or np.isnan(avg_atr) or avg_atr == 0:
                    adjusted_size = (
                        self.base_size
                    )  # Default position size if ATR is missing
                else:
                    adjusted_size = int(self.base_size / (current_atr / avg_atr))

                self.position_size = max(1, adjusted_size)  # Ensure minimum size of 1

                self.buy(size=self.position_size)
                self.has_bought = True

                print(
                    f"BUY {self.position_size} at {self.entry_price} on {current_time} (Reason: {reason})"
                )

        elif self.has_bought:
            should_exit, reason = self.should_exit()
            if should_exit:
                self.exit_reason = reason
                self.position.close()
                self.has_bought = False
                self.exit_time = current_time

                print(
                    f"EXIT at {self.data.Close[-1]} on {current_time} (Reason: {reason})"
                )
