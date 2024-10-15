import pandas as pd

from zipbird.replay.order_container import OrderContainer
from zipbird.utils.logger_util import DebugLogger
from zipbird.basic.order import Order, ShareOrder
from zipbird.basic.signal import Signal
from zipbird.basic.types import CloseStockNotInPortfolioException, Equity, OpenClose, Portfolio, Positions
from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.position_manager.position_manager import PositionManager
from zipbird.position_manager.position_sizer import PositionSizer
from zipbird.strategy.strategy import BaseStrategy


class StrategyExecutor:
    def __init__(self,
                 strategy: BaseStrategy,
                 position_sizer:PositionSizer):
        self.strategy = strategy
        self.position_sizer = position_sizer
        self.pipeline_maker = PipelineMaker()
        self.replay_order_container = OrderContainer(self.strategy.get_name())
        
    def init(self, debug_logger:DebugLogger):
        self.debug_logger = debug_logger        
        self.position_manager = PositionManager(self.debug_logger, self.replay_order_container)

    def get_order_fill_callback(self):
        return self.position_manager.on_order_filled
    
    def print_params(self):
        print(f'-------{self.strategy.__class__}-----')
        for param, p in self.strategy.get_params().items():
            print(f'{param}: {p}')

    def make_pipeline(self):
        self.strategy.prepare_pipeline_columns(self.pipeline_maker)
        return self.pipeline_maker.make_pipeline()
    
    def run(self,
            portfolio:Portfolio,
            pipeline_data:pd.DataFrame):
        self.position_manager.do_maintenance(
            portfolio.today,
            portfolio.positions, pipeline_data)

        filtered_pipeline_data = self.pipeline_maker.get_data_after_filter(pipeline_data)
        # Generate signals
        signals = self.strategy.generate_signals(
            positions=portfolio.positions, 
            pipeline_data=pipeline_data,
            filtered_pipeline_data=filtered_pipeline_data)
        
        to_open, to_close = _split_signals(signals)
        self.debug_logger.debug_print(5, f'To open signals: {len(to_open)}, {to_open}')
        self.debug_logger.debug_print(5, f'To close signals: {len(to_close)}, {to_close}')
        # calculate expected cash after closing positions
        portfolio.add_expected_cash(_get_extra_cash_after_closing(
            positions=portfolio.positions,
            to_close=to_close,
            pipeline_data=pipeline_data))
        
        # Create open orders
        open_orders = self.position_sizer.get_orders(
            portfolio=portfolio,
            signals=to_open,
            pipeline_data=pipeline_data)
        # create closing orders
        close_orders = _create_closing_orders(
            positions=portfolio.positions,
            to_close=to_close)

        self.debug_logger.log_candidates_from_pipeline(
            debug_level=2,
            pipeline_data=pipeline_data,
            orders=open_orders + close_orders)

        # Send orders
        self.position_manager.send_orders(open_orders + close_orders)


def _split_signals(signals:list[Signal]) -> tuple[list[Signal], list[Signal]]:
    to_open = []
    to_close = []
    for signal in signals:
        if signal.open_close == OpenClose.Open:
            to_open.append(signal)
        elif signal.open_close == OpenClose.Close:
            to_close.append(signal)
    return to_open, to_close


def _get_extra_cash_after_closing(positions:Positions, 
                            to_close:list[Signal],
                            pipeline_data:pd.DataFrame) -> float:
    extra_cash = 0
    for close_signal in to_close:
        stock = close_signal.stock
        if stock in positions:
            last_close = pipeline_data['close'][stock]
            extra_cash += positions[stock].amount * last_close
        else:
            raise CloseStockNotInPortfolioException(f'{stock} not in portfolio')
    return extra_cash


def _create_closing_orders(positions:Positions,
                            to_close:list[Signal]) -> list[Order]:
    orders = []
    for close_signal in to_close:
        stock = close_signal.stock
        if stock in positions:
            orders.append(ShareOrder(
                stock, 
            OpenClose.Close, 
            close_signal.long_short,
                amount=positions[stock].amount))
        else:
            raise CloseStockNotInPortfolioException(f'{stock} not in portfolio')
    return orders

