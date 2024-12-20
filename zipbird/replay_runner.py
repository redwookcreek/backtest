
import time
from functools import partial, wraps
import pandas as pd
import argparse
import zipline
import zipline.api as zipline_api
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.finance.slippage import DailyBarReplayNoSplippage

from zipbird.replay.order_collector import OrderCollector
from zipbird.replay.replay_order import ReplayOrder
from zipbird.replay.replay_strategy import ReplayStrategy
import zipbird.strategies.models as se_models
from zipbird.utils import logger_util, utils
from zipbird.utils.runner_util import supress_warnings, timing
from zipbird.utils.timer_context import TimerContext
from zipbird.notebook.performance_summary import output_performance

supress_warnings()

def run():
    parser = argparse.ArgumentParser(
        prog='StrategyRunner',
        description='Runs back test strategies')
    parser.add_argument('-s', '--start', default='2015-01-01')
    parser.add_argument('-e', '--end', default='2015-12-31')
    parser.add_argument('-d', '--debug_level', default=1)
    parser.add_argument('-c', '--capital', default=100_000)
    parser.add_argument('-b', '--bundle', default='quandl')
    parser.add_argument('-f', '--frequency', default='d')
    parser.add_argument('-l', '--label', default='')
    parser.add_argument('--replay_strategies', 
                        nargs='+',
                        type=str,
                        help='List of strategy names to replay')
    parser.add_argument('--replay_weights',
                        nargs='+',
                        type=float,
                        help='List of portfolio weights when replaying stratgies')
    
    args = parser.parse_args()

    start_time = pd.Timestamp(args.start)
    end_time = pd.Timestamp(args.end)
    
    strategy_names = args.replay_strategies
    strategy_weights = args.replay_weights
    if (not strategy_names or 
        not strategy_weights or
        not len(strategy_names) == len(strategy_weights)):
        print('To replay, must provide both strategy_names and matching strategy_weights')
        return
    for strategy_name in strategy_names:
        if strategy_name not in se_models.STRATEGY_FUNC_MAP.keys():
            print('Strategy name {} unknown, choose from: {}'.format(
                strategy_name, se_models.STRATEGY_FUNC_MAP.keys()))
            return
    strategies = [se_models.STRATEGY_FUNC_MAP[name] for name in strategy_names]

    timer_context = TimerContext()
    debug_logger = logger_util.DebugLogger(debug_level=int(args.debug_level))

    replayer = ReplayStrategy(
        strategies,
        strategy_weights,
        debug_logger=debug_logger,
        timer_context=timer_context
    )

    add_past_orders(replayer, strategy_names, start_time, end_time, '') #, args.label)
    perf = run_internal(start_time,
                 end_time,
                 replayer,
                 float(args.capital),
                 args.bundle)
    utils.dump_pickle(
            'replay',
            start_time,
            end_time,
            perf,            
            None,
            args.label)
    order_collector = OrderCollector('replay')
    for orders in replayer.orders.values():
        for order in orders:
            order_collector.add_round_trip(order)
    output_performance(
        prefix='replay',
        start_date=start_time,
        end_date=end_time,
        perf=perf,
        strategy_name='replay',
        strategy_params=None,
        label=args.label,
        bundle=args.bundle,
        replay_orders=order_collector,    
    )

def add_past_orders(
        replayer: ReplayStrategy,
        strategy_names: list[str],
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        label: str) -> list[ReplayOrder]:
    for strategy_name in strategy_names:
        with replayer.timer_context.timer('read files'):
            filename = utils.replay_filename(
                strategy_name, start_time, end_time, label)
        with replayer.timer_context.timer('load orders'):
            replayer.load_orders(filename)

@timing
def run_internal(start_time:pd.Timestamp,
                 end_time:pd.Timestamp,
                 replayer:ReplayStrategy,
                 capital:float,
                 bundle:str):
    def report_status(context, perf):
        utils.print_stats(context, perf)
        replayer.timer_context.report()

    return zipline.run_algorithm(
        start=start_time,
        end=end_time,
        initialize=partial(initialize_zipline, replayer),
        before_trading_start=partial(before_trading_start_zipline, replayer),
        analyze=report_status,
        capital_base=capital,
        data_frequency='daily',
        bundle=bundle,
    )


_PIPELINE_NAME = 'replay_pipeline'

def make_pipeline():
    return Pipeline(columns={'close': USEquityPricing.close.latest})

def initialize_zipline(replayer:ReplayStrategy,
                       context):
    context.replayer = replayer
    replayer.init()
    zipline_api.attach_pipeline(
        make_pipeline(),
        _PIPELINE_NAME,
        chunks=9000,
        eager=True
    )
    slippage_model = DailyBarReplayNoSplippage(
        callback=replayer.order_fill_callback)
    zipline_api.set_slippage(slippage_model)


def before_trading_start_zipline(replayer:ReplayStrategy, 
                                 context:zipline.TradingAlgorithm,
                                 data):
    today = zipline_api.get_datetime().date()

    # Print positions
    debug_logger = replayer.debug_logger
    debug_logger.debug_print(2, f'--------- Positions at {today} -----')
    debug_logger.debug_print(2, ','.join(f'{stock.symbol}:{pos.amount}'
                                         for stock, pos in context.portfolio.positions.items()))
    portfolio_value = context.portfolio.portfolio_value
    pipeline_data = zipline_api.pipeline_output(_PIPELINE_NAME)
    replayer.verify_all_orders_filled()
    replayer.send_orders(trade_day=today,
                         portfolio_value=portfolio_value,
                         pipeline_data=pipeline_data)


if __name__ == '__main__':
    run()