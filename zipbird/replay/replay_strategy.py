from collections import defaultdict
from datetime import date
import pandas as pd

from zipbird.utils.logger_util import DebugLogger
from zipbird.basic.types import LongShort
from zipbird.replay.replay_order import ReplayOrder
from zipbird.strategy.strategy_executor import StrategyExecutor

import zipline.api as zipline_api

class InvalidOpenOrderException(Exception):
    pass

class InvalidTradeDayOrder(Exception):
    pass

class InvalidReplayShare(Exception):
    pass

class PendingOrderNotFoundError(Exception):
    pass

class PendingOrderNotFilledError(Exception):
    pass

class ReplayStrategy:
    # strategy_name => weight mapping
    strategy_weight: dict[str, float]
    # order date => list of orders for that day
    orders: dict[date, list[ReplayOrder]]
    strategies: dict[str, StrategyExecutor]
    debug_logger: DebugLogger

    def __init__(self, 
                 strategys:list[StrategyExecutor],
                 weights:list[float]):
        self.strategy_weight = {}
        self.strategies = {}
        for strategy, weight in zip(strategys, weights):
            name = strategy.strategy.get_name()
            self.strategy_weight[name] = weight
            self.strategies[name] = strategy

        self.orders = defaultdict(list)
        self.zipline_api = zipline_api
        self.pending_orders = {}

    def init(self, debug_logger:DebugLogger):
        self.debug_logger = debug_logger
        for day_orders in self.orders.values():
            for order in day_orders:                
                order.asset = self.zipline_api.symbol(order.symbol)
    
    def _load_one_file_from_csv(self, filename:str) -> list[ReplayOrder]:
        result = []
        with open(filename) as f:
            for line in f:
                order = ReplayOrder.from_csv(line)
                result.append(order)
        return result
    
    def load_orders(self, filename:str) -> None:
        for order in self._load_one_file_from_csv(filename):
            self.orders[order.open_date].append(order)
            if order.close_date:
                self.orders[order.close_date].append(order)

    def order_fill_callback(self,
                            asset,
                            price:float,
                            amount:int,
                            order):
        self.debug_logger.debug_print(
            5, f'Order fill {asset} {amount} at {price:.4f}')
        self.debug_logger.debug_print(
            6, f'Orginal order {order}')
        if order.id not in self.pending_orders:
            raise PendingOrderNotFoundError(f'Order {order} could not be found in pending orders')
        else:
            self.pending_orders.pop(order.id)

    def verify_all_orders_filled(self):
        if self.pending_orders:
            pending = [ f'{zipline_id} : {order}' for zipline_id, order in self.pending_orders.items()]   
            raise PendingOrderNotFilledError(f'Orders not filled from yesterday: {",".join(pending)}')


    def send_orders(self, trade_day:date, portfolio_value:float, pipeline_data:pd.DataFrame):
        for order in self.orders[trade_day]:
            if order.open_date == trade_day:
                if order.open_sizer_percent and order.open_sizer_percent > 0:
                    order.replay_shares = self.get_open_percent_order_share(
                        order, portfolio_value, pipeline_data)
                elif order.open_sizer_stop_diff and order.open_sizer_stop_diff > 0:
                    order.replay_shares = self.get_open_stop_diff_order_share(
                        order, portfolio_value, pipeline_data)
                else:
                    raise InvalidOpenOrderException(
                        "Order must have either open_sizer_percent or open_sizer_stop_diff")
                self.send_open_order(order)
            elif order.close_date == trade_day:
                if order.replay_shares == 0:
                    raise InvalidReplayShare("Must be non zero shares to close an order")
                self.send_close_order(order)
            else:
                raise InvalidTradeDayOrder("Order date must match trade day")

    def get_open_percent_order_share(
            self, order:ReplayOrder, portfolio_value:float, pipeline_data:pd.DataFrame):
        weight = self.strategy_weight[order.strategy_name]
        target_value = order.open_sizer_percent * weight * portfolio_value
        shares = int(target_value / pipeline_data['close'][order.asset])
        return shares

    def get_open_stop_diff_order_share(
            self, order:ReplayOrder, portfolio_value:float, pipeline_data:pd.DataFrame):
        weight = self.strategy_weight[order.strategy_name]
        position_sizer = self.strategies[order.strategy_name].position_sizer
        last_close = pipeline_data['close'][order.asset]
        # each position should not exceed 10% of the portfolio
        max_value_by_percent = (
            position_sizer.get_max_equity_per_position() * weight * portfolio_value)
        shares_by_percent = int(max_value_by_percent / last_close)
        # shares by risk
        value_at_risk = position_sizer.get_max_fraction_risk() * weight * portfolio_value
        shares_by_risk = int(value_at_risk / order.open_sizer_stop_diff)
        return min(shares_by_percent, shares_by_risk)

    def send_open_order(self, order:ReplayOrder):
        sign = 1 if order.long_short == LongShort.Long else -1
        zipline_id = self.zipline_api.order(
            order.asset,
            amount=sign * order.replay_shares,
            limit_price=order.open_price)
        self.pending_orders[zipline_id] = order
        self.debug_logger.debug_print(6, f'sending out open {zipline_id}: {order}')

    def send_close_order(self, order:ReplayOrder):
        sign = -1 if order.long_short == LongShort.Long else 1
        zipline_id = self.zipline_api.order(
            order.asset,
            amount = sign * order.replay_shares,
            limit_price=order.close_price
        )
        self.pending_orders[zipline_id] = order
        self.debug_logger.debug_print(6, f'sending out close {zipline_id}: {order}')