"""Position sizer for rotation strategy"""

import numpy as np
import pandas as pd

from zipbird.strategy import pipeline_column_names as columen_names
from zipbird.basic.order import PercentOrder, Order, ShareOrder
from zipbird.basic.signal import OpenClose, Signal
from zipbird.basic.types import Equity, Portfolio
from zipbird.position_manager.position_sizer import PositionSizer
class RotationPositionSizer(PositionSizer):

    def _get_weights(self, 
                     pipeline_output:pd.DataFrame,
                     kept_positions: list[Equity], 
                     buy_list: list[Equity], 
                     free_cash: float,
                     portfolio_value: float):
        VOL = columen_names.vol_name(self.params['vol_window'])
        if self.params['rebalance_by_vol']:
            new_portfolio = list(set(kept_positions + buy_list))
            # Calculate inverse volatility for stocks,
            # and make target position weights        
            vola_table = pipeline_output[VOL][new_portfolio]
            inv_vola_table = 1/vola_table
            sum_inv_vola = np.sum(inv_vola_table)
            vola_target_weights = inv_vola_table / sum_inv_vola
            return vola_target_weights
        elif buy_list:
            weight = free_cash / len(buy_list) / portfolio_value
            return dict((s, weight) for s in buy_list)
        else:
            return {}
        
    def get_orders(
            self,
            portfolio: Portfolio,
            signals: list[Signal], 
            pipeline_data: pd.DataFrame) -> list[Order]:
        """Returns orders and stops with proper size
        
        Rotation system does not have stops.
        """
        super().get_orders(portfolio, signals, pipeline_data)
        balance_weekday = self.params.get('balance_weekday', None)
        if balance_weekday is not None and balance_weekday != portfolio.today.weekday():
            # If set, only balance on specific weekday
            # Otherwise, balance every day
            return []
        
        buy_stock_list = [s.stock for s in signals if s.open_close == OpenClose.Open]
        kept_stock_list = [s.stock for s in signals if s.open_close == OpenClose.Adjust]

        weights = self._get_weights(
            pipeline_output=pipeline_data,
            kept_positions=kept_stock_list,
            buy_list=buy_stock_list,
            free_cash=portfolio.get_cash_after_close(),
            portfolio_value=portfolio.portfolio_value)
        return [
            PercentOrder.make_open_long(stock, pct, stock in kept_stock_list) 
            for stock, pct in weights.items()
        ]
        