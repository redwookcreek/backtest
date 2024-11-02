
import time
from functools import partial, wraps
import pandas as pd
import argparse
import zipline
import zipline.api as zipline_api
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.finance.slippage import DailyBarReplayNoSplippage

from zipbird.replay.replay_order import ReplayOrder
from zipbird.replay.replay_strategy import ReplayStrategy
import zipbird.strategies.models as se_models
from zipbird.utils import logger_util, utils

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning, append=True)
# supress empyrical warnings "invalid value encountered in divide"
warnings.simplefilter(action='ignore', category=RuntimeWarning, lineno=710, append=True)
warnings.simplefilter(action='ignore', category=RuntimeWarning, lineno=799, append=True)



def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        start = time.perf_counter()
        result = f(*args, **kw)
        seconds = time.perf_counter() - start
        minutes = seconds / 60
        print('func:%r took: %.2f minutes' % \
          (f.__name__, minutes))
        return result
    return wrap

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
    replayer = ReplayStrategy(
        strategies,
        strategy_weights,
    )
    add_past_orders(replayer, strategy_names, start_time, end_time, '') #, args.label)
    perf = run_internal(start_time,
                 end_time,
                 replayer,
                 float(args.capital),
                 int(args.debug_level),
                 args.bundle)
    utils.dump_pickle(
            'replay',
            start_time,
            end_time,
            perf,            
            None,
            args.label)

def add_past_orders(
        replayer: ReplayStrategy,
        strategy_names: list[str],
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        label: str) -> list[ReplayOrder]:
    for strategy_name in strategy_names:
        filename = utils.replay_filename(
            strategy_name, start_time, end_time, label)
        replayer.load_orders(filename)

@timing
def run_internal(start_time:pd.Timestamp,
                 end_time:pd.Timestamp,
                 replayer:ReplayStrategy,
                 capital:float,
                 debug_level:int,
                 bundle:str):
    debug_logger = logger_util.DebugLogger(debug_level=debug_level)
    return zipline.run_algorithm(
        start=start_time,
        end=end_time,
        initialize=partial(initialize_zipline, replayer, debug_logger),
        before_trading_start=partial(before_trading_start_zipline, replayer),
        analyze=utils.print_stats,
        capital_base=capital,
        data_frequency='daily',
        bundle=bundle,
    )


_PIPELINE_NAME = 'replay_pipeline'

def make_pipeline():
    return Pipeline(columns={'close': USEquityPricing.close.latest})

def initialize_zipline(replayer:ReplayStrategy, debug_logger:logger_util.DebugLogger, context):
    context.replayer = replayer
    replayer.init(debug_logger)
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
    portfolio_value = context.portfolio.portfolio_value
    pipeline_data = zipline_api.pipeline_output(_PIPELINE_NAME)
    replayer.verify_all_orders_filled()
    replayer.send_orders(trade_day=today,
                         portfolio_value=portfolio_value,
                         pipeline_data=pipeline_data)


if __name__ == '__main__':
    run()