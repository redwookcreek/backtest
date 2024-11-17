
from functools import partial
import pandas as pd
import argparse
import zipline
import zipline.api as zipline_api
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing

import zipbird.strategies.models as se_models
from zipbird.strategy.strategy_zipline_funcs import before_trading_start_use_loader, load_initialize_zipline_use_loader
from zipbird.utils import logger_util, utils
from zipbird.utils.runner_util import supress_warnings, timing
from zipbird.utils.timer_context import TimerContext
from zipbird.strategy.pipeline_saver import PipelineSaver
from zipbird.strategy.pipeline_loader import PipelineLoader
from zipbird.strategy.pipleine_const import get_db_conn, has_db_file

supress_warnings()

def run():
    parser = argparse.ArgumentParser(
        prog='StrategyRunner',
        description='Runs back test strategies')
    parser.add_argument('command', default='dump', choices=['dump', 'load'])
    parser.add_argument('-s', '--start', default='2015-01-01')
    parser.add_argument('-e', '--end', default='2015-12-31')
    parser.add_argument('-d', '--debug_level', default=1)
    parser.add_argument('-b', '--bundle', default='norgatedata-debug')
    parser.add_argument('-l', '--label', default='',
                        help='label for output files')
    parser.add_argument('--db_name', default='pipeline-data',
                        help='database file name, will be under results/<db_name>.db')
    parser.add_argument('--start_fresh_db', default=False,
                        help='If true erase existing db table and start fresh')
    parser.add_argument('--strategies', 
                        nargs='+',
                        type=str,
                        help='List of strategy names to collect pipeline data')
    
    args = parser.parse_args()

    start_time = pd.Timestamp(args.start)
    end_time = pd.Timestamp(args.end)
    
    strategy_names = args.strategies
    if (not strategy_names):
        print('To replay, must provide list of strategy_names')
        return
    for strategy_name in strategy_names:
        if strategy_name not in se_models.STRATEGY_FUNC_MAP.keys():
            print('Strategy name {} unknown, choose from: {}'.format(
                strategy_name, se_models.STRATEGY_FUNC_MAP.keys()))
            return
    strategies = [se_models.STRATEGY_FUNC_MAP[name] for name in strategy_names]
    timer_context = TimerContext()

    if args.command == 'dump':
        pipeline_saver = PipelineSaver(
            strategies,
            get_db_conn(args.db_name),
            start_fresh=args.start_fresh_db)

        run_dump_internal(
            timer_context,
            start_time,
            end_time,
            pipeline_saver,
            int(args.debug_level),
            args.bundle)
        with timer_context.timer('create index'):
            pipeline_saver.create_index()

    elif args.command == 'load':
        if len(strategies) > 1:
            print('Can load only one strategy')
            return
        
        if not has_db_file(args.db_name):
            print(f'No database found for {args.db_name}')
            return
        
        pipeline_loader = PipelineLoader(
            strategies[0],
            get_db_conn(args.db_name))
        
        run_load_internal(
            timer_context,
            start_time,
            end_time,
            pipeline_loader,
            int(args.debug_level),
            args.bundle)
        
        utils.dump_replay_orders(
            strategy_names[0],
            start_time,
            end_time,
            strategies[0],
            args.label
        )
    else:
        raise ValueError(f'Unknown command {args.command}')

    timer_context.report()

############################
# DUMP pipeline data to database
############################
@timing
def run_dump_internal(
        timer_context: TimerContext,
        start_time:pd.Timestamp,
        end_time:pd.Timestamp,
        pipeline_saver:PipelineSaver,
        debug_level:int,
        bundle:str):
    debug_logger = logger_util.DebugLogger(debug_level=debug_level)
    pipeline_saver.init(debug_logger, start_time, end_time)
    return zipline.run_algorithm(
        start=start_time,
        end=end_time,
        initialize=partial(dump_initialize_zipline, pipeline_saver, timer_context, debug_logger),
        before_trading_start=partial(dump_before_trading_start_zipline, pipeline_saver, timer_context),
        analyze=utils.print_stats,
        capital_base=100_000,
        data_frequency='daily',
        bundle=bundle,
    )


_PIPELINE_NAME = 'collect_pipeline'

def dump_make_pipeline():
    return Pipeline(columns={'close': USEquityPricing.close.latest})

def dump_initialize_zipline(pipeline_saver:PipelineSaver,
                            timer_context: TimerContext,
                            debug_logger:logger_util.DebugLogger,
                            context):
    context.pipeline_saver = pipeline_saver    
    zipline_api.attach_pipeline(
        pipeline_saver.make_pipeline(),
        _PIPELINE_NAME,
        chunks=500,
        eager=True
    )

def dump_before_trading_start_zipline(pipeline_saver:PipelineSaver, 
                                      timer_context: TimerContext,
                                      context:zipline.TradingAlgorithm,
                                      data):
    today = zipline_api.get_datetime().date()
    with timer_context.timer('calc_pipeline'):
        pipeline_data = zipline_api.pipeline_output(_PIPELINE_NAME)

    with timer_context.timer('record_pipeline_data'):
        pipeline_saver.record_pipeline_data(
            trade_day=today, pipeline_data=pipeline_data)


##############################
# Load from db to run strategy
##############################
@timing
def run_load_internal(timer_context: TimerContext,
                      start_time:pd.Timestamp,
                      end_time:pd.Timestamp,
                      pipeline_loader:PipelineLoader,
                      debug_level:int,
                      bundle:str):    
    debug_logger = logger_util.DebugLogger(debug_level=debug_level)
    return zipline.run_algorithm(
        start=start_time,
        end=end_time,
        initialize=partial(load_initialize_zipline_use_loader, pipeline_loader, timer_context, debug_logger),
        before_trading_start=partial(before_trading_start_use_loader, pipeline_loader, timer_context),
        analyze=utils.print_stats,
        capital_base=100_000,
        data_frequency='daily',
        bundle=bundle,
    )

    
if __name__ == '__main__':
    run()