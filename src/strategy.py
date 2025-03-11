from backtesting import Strategy
import numpy as np
import talib

class SimpleStraddleStrategy(Strategy):
    hold_period = 3  # Hold for 3 days
    cooldown_period = 1  # Wait 1 day before repurchasing
    profit_target = 0.20  # 20% profit target
    stop_loss = -0.10  # 10% stop loss
    min_iv = 0.2  # Minimum implied volatility threshold
    max_iv = 0.7  # Avoid buying options with extreme IV

    def init(self):
        self.has_bought = False
        self.entry_price = None
        self.entry_time = None
        self.exit_time = None  
        self.entry_reason = None  
        self.exit_reason = None  

        # Indicators
        self.rsi = self.I(talib.RSI, self.data.Close, timeperiod=14)
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, timeperiod=14)
        self.iv = self.data.impliedVolatility

    def should_buy(self):
        """
        Determines if we should enter a straddle position.
        - Filters for volume and IV conditions.
        - Uses RSI & ATR for entry timing.
        """
        reasons = []
        if self.iv[-1] < self.min_iv:
            return False, "Implied Volatility too low"
        if self.iv[-1] > self.max_iv:
            return False, "Implied Volatility too high"
        if self.data.volume[-1] == 0:
            return False, "No trading volume"

        if self.rsi[-1] < 30:
            reasons.append("RSI Oversold")
        if self.rsi[-1] > 70:
            reasons.append("RSI Overbought")
        if self.atr[-1] > self.atr[-2]:
            reasons.append("Increasing ATR (Volatility)")

        if reasons:
            return True, ", ".join(reasons)
        return False, ""

    def should_exit(self):
        """
        Determines if we should exit the position.
        """
        reasons = []
        price_change = (self.data.Close[-1] - self.entry_price) / self.entry_price

        if self.data.index[-1] >= self.entry_time + np.timedelta64(self.hold_period, "D"):
            reasons.append(f"Hold Period Expired ({self.hold_period} days)")
        if price_change >= self.profit_target:
            reasons.append(f"Profit Target Hit (+{self.profit_target * 100}%)")
        if price_change <= self.stop_loss:
            reasons.append(f"Stop Loss Hit ({self.stop_loss * 100}%)")

        if reasons:
            return True, ", ".join(reasons)
        return False, ""

    def next(self):
        current_time = self.data.index[-1]

        if self.exit_time and current_time < self.exit_time + np.timedelta64(self.cooldown_period, "D"):
            return  # Skip buying if still in cooldown period

        if not self.has_bought:
            should_buy, reason = self.should_buy()
            if should_buy:
                self.entry_price = self.data.Close[-1]
                self.entry_time = current_time
                self.entry_reason = reason
                self.buy()
                self.has_bought = True

        elif self.has_bought:
            should_exit, reason = self.should_exit()
            if should_exit:
                self.exit_reason = reason
                self.position.close()
                self.has_bought = False
                self.exit_time = current_time
