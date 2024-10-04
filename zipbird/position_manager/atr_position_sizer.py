import pandas as pd

from zipbird.basic.order import Order, ShareOrder
from zipbird.basic.signal import Signal
from zipbird.basic.stop import FixStop, PercentProfitTarget, StopOrder
from zipbird.basic.types import Equity, Portfolio
from zipbird.strategy import pipeline_column_names as colume_names
from zipbird.position_manager.position_sizer import PositionSizer

class ATRPositionSizer(PositionSizer):
    def __init__(self, params):
        self.params = params
    
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
            
            target_percent = self.params.get('price_target_percent', 0)
            if target_percent:
                target = PercentProfitTarget(signal.long_short, target_percent)
            else:
                target = None
            # Attach stop loss
            order.add_stop(StopOrder(
                initial_stop=FixStop(
                    signal.long_short,
                    self._get_stop_loss_diff(signal.stock, pipeline_data)),
                time_stop=self.params.get('stop_loss_days', 0),
                profit_target=target))
            orders.append(order)                          
        return orders
                    
    def _get_stop_loss_diff(self, stock:Equity, pipeline_data:pd.DataFrame):
        atr_name = colume_names.atr_name(self.params['atr_period'])
        atr = pipeline_data[atr_name][stock]
        return self.params['stop_loss_atr_multiple'] * atr

    def _get_amount(self, stock:Equity, portfolio_value:float, pipeline_data:pd.DataFrame):
        price = pipeline_data['close'][stock]
        risk = self._get_stop_loss_diff(stock, pipeline_data)
        return min(
            int(portfolio_value * self.params['fraction_risk'] / risk),
            int(portfolio_value * self.params['max_equity_per_position'] / price))