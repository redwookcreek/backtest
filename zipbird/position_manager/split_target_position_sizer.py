import pandas as pd

from zipbird.basic.order import Order
from zipbird.basic.signal import Signal
from zipbird.basic.types import Portfolio
from zipbird.position_manager.atr_position_sizer import ATRPositionSizer


class SplitTargetPositionSizer(ATRPositionSizer):
    """Position Sizer that split the order into two, one with a target, one with 
    a trailing stop
    """
    def get_orders(self,
                   portfolio: Portfolio,
                   signals: list[Signal], 
                   pipeline_data: pd.DataFrame) -> list[Order]:
        org_orders = super().get_orders(portfolio, signals, pipeline_data)
        orders = []
        for order in org_orders:
            # order 1 no profit target
            new_order1 = order.copy()
            new_order1.amount = (new_order1.amount // 3) 
            new_order1.stop.profit_target = None

            # order 2 has profit target
            new_order2 = order.copy()
            new_order2.amount = (new_order2.amount // 3) * 2
            
            orders.append(new_order1)
            orders.append(new_order2)
        return orders