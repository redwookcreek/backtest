import pandas as pd
from zipbird.basic.order import Order, ShareOrder
from zipbird.basic.signal import Signal
from zipbird.basic.types import Portfolio
from zipbird.position_manager.position_sizer import PositionSizer


class FullPositionSizer(PositionSizer):
    """Position sizer that put 100% portoflio to the position"""
    def __init__(self):
        pass
    
    def get_orders(self,
                   portfolio: Portfolio,
                   signals: list[Signal], 
                   pipeline_data: pd.DataFrame) -> list[Order]:
        if not signals:
            return []
        closes = pipeline_data['close']
        # Just evenly split among positions in the portfolio
        value_each_position = portfolio.portfolio_value / (len(portfolio.positions) + len(signals))
        return [
            ShareOrder(
                stock=signal.stock,
                open_close=signal.open_close,
                long_short=signal.long_short,
                amount=value_each_position / closes[signal.stock])
            for signal in signals
        ]