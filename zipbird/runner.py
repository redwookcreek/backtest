
import time
from functools import partial, wraps
import pandas as pd
import argparse
import zipline
import pickle

import zipbird.strategies.models as se_models
from zipbird.strategy.strategy_zipline_funcs import initialize_zipline, before_trading_start_zipline
from zipbird.utils import logger_util, utils
from zipbird.runner_util import supress_warnings, timing

supress_warnings()

def run():
    parser = argparse.ArgumentParser(
        prog='StrategyRunner',
        description='Runs back test strategies')
    parser.add_argument('strategy_name')
    parser.add_argument('-a', '--action', default='run', choices=['run', 'verify'])
    parser.add_argument('-s', '--start', default='2015-01-01')
    parser.add_argument('-e', '--end', default='2015-12-31')
    parser.add_argument('-u', '--test_universe', default='')
    parser.add_argument('-d', '--debug_level', default=1)
    parser.add_argument('-c', '--capital', default=100_000)
    parser.add_argument('-b', '--bundle', default='quandl')
    parser.add_argument('-f', '--frequency', default='d')
    parser.add_argument('-l', '--label', default='')
    
    args = parser.parse_args()

    
    strategy = se_models.STRATEGY_FUNC_MAP[args.strategy_name]
    start_time = pd.Timestamp(args.start)
    end_time = pd.Timestamp(args.end)
    print(f'========== params ==========')
    strategy.print_params()
    
    print(f'bundle: {args.bundle}')
    print(f'start: {args.start}')
    print(f'end: {args.end}')
    action = args.action
    if action == 'run':
        #round_trip_tracker = position_manager.RoundTripTracker()
        if args.strategy_name not in se_models.STRATEGY_FUNC_MAP:
            print('Strategy name {} unknown, choose from: {}'.format(
                args.strategy_name, se_models.STRATEGY_FUNC_MAP.keys()))
            return
        perf = run_internal(
            start_time=start_time,
            end_time=end_time,
            strategy=strategy,            
            capital=args.capital,
            debug_level=int(args.debug_level),
            bundle=args.bundle)
        
        utils.dump_pickle(
            args.strategy_name,
            start_time,
            end_time,
            perf,            
            strategy,
            args.label)
        
        utils.dump_replay_orders(
            args.strategy_name,
            start_time,
            end_time,
            strategy,
            args.label
        )

    elif action == 'verify':
        verify_strategy_result(
            prefix=args.strategy_name,
            start_date=start_time,
            end_date=end_time,
            strategy=strategy,
            label=args.label
        )

@timing
def run_internal(start_time, end_time, strategy, capital, debug_level, bundle):
    debug_logger = logger_util.DebugLogger(debug_level=debug_level)
    strategy.init(debug_logger)
    return zipline.run_algorithm(
        start=start_time,
        end=end_time,
        initialize=partial(initialize_zipline, strategy, debug_logger),
        before_trading_start=partial(before_trading_start_zipline, strategy),
        analyze=utils.print_stats,
        capital_base=capital,
        data_frequency='daily',
        bundle=bundle,
    )

@timing
def verify_strategy_result(prefix:str, start_date:pd.Timestamp, end_date:pd.Timestamp, strategy_class, label):
    filename = utils.pickle_filename(prefix, start_date, end_date, label)
    with open(filename, 'rb') as file:
        loaded = pickle.load(file)
        #perf = loaded['perf']
        round_trip_tracker = loaded['round_trip_tracker']
        params = loaded['params']
        for round_trip in round_trip_tracker.round_trips():
            msg = strategy_class.verify_round_trip(round_trip, params)
            if msg:
                print('--------------------')
                print(pd.Series(round_trip_tracker.round_trip_to_dict(round_trip)))
                print('\n'.join(msg))

if __name__ == '__main__':
    run()