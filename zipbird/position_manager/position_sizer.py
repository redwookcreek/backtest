"""Position Manager"""

import pandas as pd

from zipbird.basic.types import ClosingInPositionSizerException, OpenClose, Portfolio
from zipbird.basic.signal import Signal
from zipbird.basic.order import Order



class PositionSizer:

    def get_orders(
            self,
            portfolio: Portfolio,
            signals: list[Signal], 
            pipeline_data: pd.DataFrame) -> list[Order]:
        """Returns orders and stops with proper size"""
        for signal in signals:
            if signal.open_close == OpenClose.Close:
                raise ClosingInPositionSizerException('Should not close position in position sizer')