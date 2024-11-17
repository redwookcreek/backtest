from functools import cache

import pandas as pd
import zipline.api as zipline_api
from zipline.algorithm import TradingAlgorithm
from zipline.finance.slippage import DailyBarNoSplippage

from zipbird.utils import logger_util
from zipbird.utils.timer_context import TimerContext
from zipbird.basic.types import Portfolio
from zipbird.strategy.pipeline_loader import PipelineLoader
from zipbird.strategy.strategy_executor import StrategyExecutor

_STRATEGY_PIPELINE_NAME = 'backtest_strategy_pipeline'

def initialize_zipline(strategy_executor, debug_logger, context):   

    zipline_api.attach_pipeline(
        strategy_executor.make_pipeline(),
        _STRATEGY_PIPELINE_NAME,
        chunks=3000,
        eager=True)
    _init_internal(strategy_executor, debug_logger, context)

def _init_internal(strategy_executor:StrategyExecutor,
                   debug_logger:logger_util.DebugLogger,
                   context:TradingAlgorithm):
    zipline_api.set_benchmark(zipline_api.symbol('$SPX'))
    slippage_model = DailyBarNoSplippage(
        callback=strategy_executor.get_order_fill_callback())
    zipline_api.set_slippage(slippage_model)
    context.debug_logger = debug_logger

def before_trading_start_zipline(strategy_executor:StrategyExecutor, context:TradingAlgorithm, data):
    pipeline_data = zipline_api.pipeline_output(_STRATEGY_PIPELINE_NAME)
    _run_for_one_day(strategy_executor, context, pipeline_data, False)

def _run_for_one_day(strategy_executor:StrategyExecutor, context:TradingAlgorithm, pipeline_data:pd.DataFrame, use_pipeline_loader:bool):
    context.debug_logger.output_progress(context, pipeline_data, strategy_executor)
    portfolio = Portfolio(
        today=zipline_api.get_datetime().date(),
        portfolio_value=context.portfolio.portfolio_value,
        portfolio_cash=context.portfolio.cash,
        positions=context.portfolio.positions)
    strategy_executor.run(
        portfolio=portfolio,
        pipeline_data=pipeline_data,
        use_pipeline_loader=use_pipeline_loader
    )

@cache
def sid_to_zipline_symbol(sid):    
    return zipline_api.sid(sid)

@cache
def ticker_to_zipline_symbol(ticker):
    return zipline_api.symbol(ticker)

def before_trading_start_use_loader(pipeline_loader:PipelineLoader,
                                    timer_context: TimerContext,
                                    context:TradingAlgorithm,
                                    data):
    today = zipline_api.get_datetime().date()
    with timer_context.timer('load_pipeline_data'):
        pipeline_data = pipeline_loader.load_for_trade_day(trade_day=today)
    
    # loaded index are tickers, convert them to zipline symbol
    with timer_context.timer('convert to symbol'):        
        pipeline_data.index = pipeline_data.index.map(sid_to_zipline_symbol)

    with timer_context.timer('run trading'):
        _run_for_one_day(strategy_executor=pipeline_loader.strategy,
                         context=context,
                         pipeline_data=pipeline_data,
                         use_pipeline_loader=True)
        
def load_initialize_zipline_use_loader(pipeline_loader:PipelineLoader, 
                                       timer_context: TimerContext,
                                       debug_logger:logger_util.DebugLogger,
                                       context):
    pipeline_loader.init(debug_logger=debug_logger)
    _init_internal(pipeline_loader.strategy, debug_logger, context)
    