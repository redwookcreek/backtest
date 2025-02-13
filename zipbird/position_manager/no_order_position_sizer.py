import pandas as pd
from zipbird.basic.order import Order
from zipbird.basic.signal import Signal
from zipbird.basic.types import Portfolio
from zipbird.position_manager.position_sizer import PositionSizer


class NoOrderPositionSizer(PositionSizer):
    """Position sizer that does not create orders
    
    This is to test the strategy without sending orders.
    """
    def __init__(self):
        pass
    
    def get_orders(self,
                   portfolio: Portfolio,
                   signals: list[Signal], 
                   pipeline_data: pd.DataFrame) -> list[Order]:
        return []