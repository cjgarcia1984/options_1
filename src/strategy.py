from backtesting import Strategy
from abc import ABC, abstractmethod


class BaseStrategy(Strategy, ABC):
    @classmethod
    @abstractmethod
    def create_data(cls, engine, contracts):
        pass

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def next(self):
        pass


class SimpleStraddleStrategy(BaseStrategy):
    @classmethod
    def create_data(cls, engine, contracts):
        dfs = [
            engine.load_contract(**contract)
            for contract in contracts
        ]
        combined_df = sum(dfs)
        combined_df.dropna(inplace=True)
        return combined_df

    def init(self):
        self.has_bought = False

    def next(self):
        if not self.has_bought:
            self.buy()
            self.has_bought = True
        if len(self.data) == len(self.data.df) and self.position:
            self.position.close()
