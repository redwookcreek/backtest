from datetime import date
import pandas as pd
import numpy as np
import pickle
import math

def print_stats(context, perf):
    perf['max'] = perf.portfolio_value.cummax()
    perf['dd'] = perf.portfolio_value / perf['max'] - 1
    maxdd = perf['dd'].min()

    ann_ret = np.power(
        perf.portfolio_value.iloc[-1] / perf.portfolio_value.iloc[0],
        252 / len(perf)) - 1
    print('Annualized Return: {:.2%}, Max Drawdown: {:.2%}'.format(ann_ret, maxdd))


def filename(prefix:str, start_date:pd.Timestamp, end_date:pd.Timestamp, label:str=''):
    return '{}-{:%y-%m-%d}-to-{:%y-%m-%d}-{}'.format(
        prefix, start_date, end_date, label)

def pickle_filename(prefix:str, start_date:pd.Timestamp, end_date:pd.Timestamp, label:str=''):
    return f'results/{filename(prefix, start_date, end_date, label)}.pickle'

def dump_pickle(
        prefix:str,
        start_date:pd.Timestamp,
        end_date:pd.Timestamp,
        perf_obj,
        strategy,
        label=''):
    """Dump a pickle file to save performance data and paramters in strategy class"""
    filename = pickle_filename(prefix, start_date, end_date, label)
    with open(filename,'wb') as file:
         pickle.dump(
             {
                 'perf': perf_obj,
                 #'params': strategy.get_params(),
                 #'round_trip_tracker': round_trip_tracker
            },
            file)

def replay_filename(prefix:str, start_date:pd.Timestamp, end_date:pd.Timestamp, label:str=''):
    return f'results/replay-{filename(prefix, start_date, end_date, label)}.csv'

def dump_replay_orders(
        prefix:str,
        start_date:pd.Timestamp,
        end_date:pd.Timestamp,
        strategy,
        label=''):
    """Dump a csv file that contains replay orders"""
    filename = replay_filename(prefix, start_date, end_date, label)
    strategy.replay_order_container.write_orders(filename)

def compare_object(o1, o2):
    return (
        o1 is None and o2 is None
        or
        o1 == o2)

def get_quarter(d: date):
    return math.ceil(d.month / 3)