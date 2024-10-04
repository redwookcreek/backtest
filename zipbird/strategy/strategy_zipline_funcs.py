import zipline.api as zipline_api
from zipline.algorithm import TradingAlgorithm
from zipline.finance.slippage import DailyBarNoSplippage

from zipbird.basic.types import Portfolio

_STRATEGY_PIPELINE_NAME = 'backtest_strategy_pipeline'

def initialize_zipline(strategy_executor, debug_logger, context):
    zipline_api.set_benchmark(zipline_api.symbol('$SPX'))

    zipline_api.attach_pipeline(
        strategy_executor.make_pipeline(),
        _STRATEGY_PIPELINE_NAME,
        chunks=3000,
        eager=True)

    slippage_model = DailyBarNoSplippage(
        callback=strategy_executor.get_order_fill_callback())
    zipline_api.set_slippage(slippage_model)
    context.debug_logger = debug_logger

def before_trading_start_zipline(strategy_executor, context:TradingAlgorithm, data):
    pipeline_data = zipline_api.pipeline_output(_STRATEGY_PIPELINE_NAME)
    context.debug_logger.output_progress(context, pipeline_data, strategy_executor)
    portfolio = Portfolio(
        today=zipline_api.get_datetime().date(),
        portfolio_value=context.portfolio.portfolio_value,
        portfolio_cash=context.portfolio.cash,
        positions=context.portfolio.positions)
    strategy_executor.run(
        portfolio=portfolio,
        pipeline_data=pipeline_data,
    )