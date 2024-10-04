import datetime
from enum import Enum
import pandas as pd

class CloseStockNotInPortfolioException(Exception):
    pass

class ClosingInPositionSizerException(Exception):
    pass

class LongShort(Enum):
    Long = 1
    Short = 2


class OpenClose(Enum):
    Open = 1
    Close = 2
    Adjust = 3


class StopOrderStatus(Enum):
    NOT_TRIGGER = 1
    TIME_STOP = 2
    INITIAL_STOP = 3
    TRAILING_STOP = 4
    TARGET_REACHED = 5


class Equity:
    """
    Equity is a stock

    This is meant to mimic zipline's Position object,
    used during testing.
    """
    symbol: str
    auto_close_date: datetime.date
    def __init__(self, symbol:str, auto_close_date:datetime.date=None):
        self.symbol = symbol
        self.auto_close_date = auto_close_date
    
    def __eq__(self, other):
        if not isinstance(other, Equity):
            return False
        return self.symbol == other.symbol
    
    def __str__(self):
        return self.symbol
    
    def __hash__(self):
        return hash(self.symbol)

class Position:
    """
    Position in a portfolio

    This is meant to mimic zipline's Position object,
    used during testing.
    """
    asset: Equity
    amount: int
    start_date: datetime.date
    cost_basis: float

    def __init__(self,
                 asset:Equity,
                 amount:int,
                 cost_basis:float=0.0):
        self.asset = asset
        self.amount = amount
        self.cost_basis = cost_basis


Positions = dict[Equity, Position]

class Portfolio:
    today: datetime.date
    portfolio_value: float
    _portfolio_cash:float
    positions: Positions

    def __init__(self,
                 today:datetime.date=None,
                 portfolio_value:float=None,
                 portfolio_cash:float=0,
                 positions:Positions=None):
        self.today = today
        self.portfolio_value = portfolio_value
        self._portfolio_cash = portfolio_cash
        self.positions = positions
        self._expected_cash = 0

    def add_expected_cash(self, expected_cash:float):
        self._expected_cash += expected_cash

    def get_cash_after_close(self):
        return self._portfolio_cash + self._expected_cash
