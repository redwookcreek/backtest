import pandas as pd
import numpy as np

from zipbird.basic.order import Order, ShareOrder
from zipbird.basic.signal import Signal
from zipbird.basic.stop import FixStop, PercentProfitTarget, PercentTrailingStop, StopOrder, FixProfitTarget
from zipbird.basic.types import Equity, Portfolio
from zipbird.strategy import pipeline_column_names as colume_names
from zipbird.position_manager.position_sizer import PositionSizer

class ATRPositionSizer(PositionSizer):
    
    def get_orders(self,
                   portfolio: Portfolio,
                   signals: list[Signal], 
                   pipeline_data: pd.DataFrame) -> list[Order]:
        """Returns orders and stops with proper size"""
        # super make sure all signals are open signals
        super().get_orders(portfolio, signals, pipeline_data)
        orders = []
        for signal in signals:
            # calculate amount based on risk and max equity per position
            amount = self._get_amount(signal.stock, portfolio.portfolio_value, pipeline_data)
            order = ShareOrder(signal.stock,
                            signal.open_close,
                            signal.long_short,
                            amount=amount,
                            limit_price=signal.limit_price)
            
            # Attach stop loss
            order.add_stop(StopOrder(
                initial_stop=FixStop(
                    signal.long_short,
                    self._get_stop_loss_diff(signal.stock, pipeline_data)),
                time_stop=self.params.get('stop_loss_days', 0),
                profit_target=self._get_profit_target(signal, pipeline_data),
                trailing=self._get_tailing_stop(signal)))
            orders.append(order)                          
        return orders
                    
    def _get_atr(self, stock:Equity, pipeline_data:pd.DataFrame):
        atr_name = colume_names.atr_name(self.params['atr_period'])
        return pipeline_data[atr_name][stock]
    
    def _get_stop_loss_diff(self, stock:Equity, pipeline_data:pd.DataFrame):
        atr = self._get_atr(stock, pipeline_data)    
        if np.isnan(atr):
            # if no atr, assume 50% risk
            return pipeline_data['close'][stock] * 0.5
        else:
            return self.params['stop_loss_atr_multiple'] * atr

    def _get_amount(self, stock:Equity, portfolio_value:float, pipeline_data:pd.DataFrame):
        price = pipeline_data['close'][stock]
        risk = self._get_stop_loss_diff(stock, pipeline_data)
        return min(
            int(portfolio_value * self.get_max_fraction_risk() / risk),
            int(portfolio_value * self.get_max_equity_per_position() / price))
    
    def _get_profit_target(self, signal, pipeline_data:pd.DataFrame):
        
        target_percent = self.params.get('price_target_percent', 0)
        if target_percent:
            return PercentProfitTarget(signal.long_short, target_percent)
        
        target_atr = self.params.get('price_target_atr_multiple', 0)
        if target_atr:
            return FixProfitTarget(signal.long_short,
                                   self._get_atr(signal.stock, pipeline_data))
        return None
        
    def _get_tailing_stop(self, signal):
        trailing_percent = self.params.get('trailing_stop_percent', 0)
        if trailing_percent:
            return PercentTrailingStop(signal.long_short, trailing_percent)
        else:
            return None