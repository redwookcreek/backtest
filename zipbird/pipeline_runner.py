
from functools import partial
import pandas as pd
import argparse
import zipline
import zipline.api as zipline_api
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing

import zipbird.strategies.models as se_models
from zipbird.utils import logger_util, utils
from zipbird.runner_util import supress_warnings, timing

supress_warnings()

def run():
    parser = argparse.ArgumentParser(
        prog='StrategyRunner',
        description='Runs back test strategies')
    parser.add_argument('-s', '--start', default='2015-01-01')
    parser.add_argument('-e', '--end', default='2015-12-31')
    parser.add_argument('-d', '--debug_level', default=1)
    parser.add_argument('-b', '--bundle', default='norgatedata-debug')
    parser.add_argument('--strategies', 
                        nargs='+',
                        type=str,
                        help='List of strategy names to collect pipeline data')
    
    args = parser.parse_args()

    start_time = pd.Timestamp(args.start)
    end_time = pd.Timestamp(args.end)
    
    strategy_names = args.replay_strategies
    if (not strategy_names):
        print('To replay, must provide list of strategy_names')
        return
    strategies = [se_models.STRATEGY_FUNC_MAP[name] for name in strategy_names]    
    pipeline_runner = PipelineRunner(strategies)
    run_internal(start_time,
                 end_time,
                 pipeline_runner,
                 int(args.debug_level),
                 args.bundle)

@timing
def run_internal(start_time:pd.Timestamp,
                 end_time:pd.Timestamp,
                 pipeline_runner:PipelineRuner,
                 capital:float,
                 debug_level:int,
                 bundle:str):
    debug_logger = logger_util.DebugLogger(debug_level=debug_level)
    return zipline.run_algorithm(
        start=start_time,
        end=end_time,
        initialize=partial(initialize_zipline, pipeline_runner, debug_logger),
        before_trading_start=partial(before_trading_start_zipline, pipeline_runner),
        analyze=utils.print_stats,
        capital_base=capital,
        data_frequency='daily',
        bundle=bundle,
    )


_PIPELINE_NAME = 'collect_pipeline'

def make_pipeline():
    return Pipeline(columns={'close': USEquityPricing.close.latest})

def initialize_zipline(pipeline_runner:PipelineRunner, debug_logger:logger_util.DebugLogger, context):
    context.pipeline_runner = pipeline_runner
    pipeline_runner.init(debug_logger)
    zipline_api.attach_pipeline(
        pipeline_runner.make_pipeline(),
        _PIPELINE_NAME,
        chunks=9000,
        eager=True
    )

def before_trading_start_zipline(pipeline_runner:PipelineRunner, 
                                 context:zipline.TradingAlgorithm,
                                 data):
    today = zipline_api.get_datetime().date()

    pipeline_data = zipline_api.pipeline_output(_PIPELINE_NAME)
    pipeline_runner.record_pipeline_data(
        trade_day=today, pipeline_data=pipeline_data)


if __name__ == '__main__':
    run()