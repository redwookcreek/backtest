"""Utility for logging"""
import pandas as pd
import zipline
from zipline import TradingAlgorithm
from zipline.api import get_open_orders

from zipbird.basic.order import Order

class DebugLogger:

    def __init__(self, debug_level=0):
        self._debug_level = debug_level
        self._last_period_value = None
        self._last_period_time = None

    def debug_print(self, lvl, msg):
        if lvl <= self._debug_level:
            print(msg)

    def output_progress(self, context: TradingAlgorithm, pipeline_data: pd.DataFrame, strategy_executor):
        """Output some preformance number during backtest run.
        
        This code just prints out the past period's performance so
        that we have something to look at while backtest runs
        """
        if self._last_period_value is not None:
            # Caculate percent difference since last period
            perf_pct = (context.portfolio.portfolio_value / self._last_period_value) - 1
        else:
            perf_pct = None

        self._last_period_value = context.portfolio.portfolio_value
        
        if perf_pct is None:
            return

        today = zipline.api.get_datetime().date()
        self.debug_print(
            0, 
            '****{today} - Last Period Result {pct_chg:.2%}, '
            'positions: {position_cnt}, value: {port_value:,.0f}'.format(
                today=today,
                pct_chg=perf_pct,
                position_cnt=len(context.portfolio.positions),
                port_value=context.portfolio.portfolio_value))
        
        self.print_current_positions(context, pipeline_data, strategy_executor, level=1)
        #self.print_open_orders(level=5)


    def print_current_positions(self, context, pipeline_data, strategy_executor, level=1):
        """Print current positions"""
        if not context.portfolio.positions:
            return
        
        self.debug_print(
            level, 
            '-------------Current pos(%d)--------------: ' % len(context.portfolio.positions))
        
        positions = []
        sl_manager = strategy_executor.position_manager
        for stock, pos in context.portfolio.positions.items():
            days = -1
            stop_p = -100
            stop_loss_price = 0.0
            target_price = 0.0
            yesterday_close = pipeline_data['close'][stock]
            days = sl_manager.get_day_count(stock, pos.amount)            
            try:
                stop_loss_price = sl_manager.get_stop_price(stock, pos.amount)
                stop_p = (sl_manager.get_stop_price(stock) / yesterday_close - 1) * 100
            except Exception as e:
                stop_loss_price = -1
                stop_p = -100
            try:
                target_price = sl_manager.get_target_price(stock, pos.amount)
            except Exception:
                target_price = -1
            
            if days == -1:
                days = '-'
            if stop_p <= -100:
                stop_p = '-'
            else:
                stop_p = '{:9.2f}'.format(stop_p)
            sign = 1 if pos.amount > 0 else -1
            positions.append((
                stock.symbol,
                pos.amount,
                pos.amount * yesterday_close,
                (pos.amount * yesterday_close) / context.portfolio.portfolio_value * 100,
                sign * (yesterday_close / pos.cost_basis - 1) * 100,
                days,
                yesterday_close,
                stop_loss_price,
                stop_p,
                target_price,
            ))
        positions.sort(key=lambda p: p[4], reverse=True)
        self.debug_print(
            level, ('{:15s}' + ' {:>9s}' * 9).format(
                'Stock', 'Amount', 'Equity', 'Portf%', 'P&L', 'Days', 'Last', 'Stop', 'Stop%', 'Target'))
        for p in positions:
            self.debug_print(
                level,
                ('{:15s} {:9d} {:9.0f} {:9.2f}% {:9.2f}% {:>9} {:9.2f} {:9.2f} {:>9s}% {:9.2f}').format(*p))
        

    def print_open_orders(self, level):
        """Print out open orders"""
        # get_open_orders returns a dictionary of Equity => list[Order]
        all_open_orders = sum(get_open_orders().values(), [])
        open_order_str = ', '.join('%s: %d, %.2f' % (s.sid.symbol, s.amount or 0, s.limit or 0.0)
                                   for s in all_open_orders)
        self.debug_print(
            level,
            'Open orders ({}): {}'.format(len(all_open_orders), open_order_str))

    def log_candidates_from_pipeline(
            self,
            debug_level,
            pipeline_data,
            orders:list[Order]
            ):
        """Prints a debug message for candidates to open positions"""
        if not orders:
            return
        def get_ind(ind, s):
            if ind in pipeline_data and s in pipeline_data[ind]:
                return pipeline_data[ind][s]
            else:
                return 0
        get_open_price = lambda s: s.limit_price or 0.0
        self.debug_print(
            debug_level, 
            '-------------Candidates--------------')
        self.debug_print(
            debug_level, 
            ('{:15s} ' + ' {:>9}' * (len(pipeline_data.keys()) + 2)).format(
                'Stock', *(k.upper() for k in pipeline_data.keys()), 'AMOUNT', 'OPrice'))
        for order in orders:
            stock = order.stock
            self.debug_print(
                debug_level,
                ("{:15s} " + " {:9.2f}" * len(pipeline_data.keys()) +
                " {:>9s} {:9.4f}").format(
                stock.symbol,      
                *(get_ind(k, stock) for k in pipeline_data.keys()),
                order.amount_debug_str(),
                get_open_price(order)))
        