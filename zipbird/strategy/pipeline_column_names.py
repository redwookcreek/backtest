"""This module contains the column names used in the strategy module."""

def rsi_name(period):
    return f'rsi_{period}'


def sma_name(period):
    return f'sma_{period}'


def atr_name(period):
    return f'atr_{period}'


def atrp_name(period):
    return f'atrp_{period}'


def adx_name(period):
    return f'adx_{period}'


def vol_name(period):
    return f'vol_{period}'


def vol_percentile_name(period):
    return f'vol%tile_{period}'


def roc_name(period):
    return f'roc_{period}'


def consecutive_up_name(period):
    return f'{period}_up'

def max_in_window(period):
    return f'{period}_high'

def cross_last_high(period):
    return f'X_{period}_high'

def cross_last_high_with_large_green_bar(period, close_percent, green_bar_limit):
    return f'STRG_X_{period}_high_{close_percent:.2f}_{green_bar_limit:.2f}'

def sma_cross(period):
    return f'sma_cross_{period}'

def sma_trend(period):
    return f'sma_trend_{period}'

def dollar_volume_rank(period):
    return f'dv_rank_{period}'

def index(index_name):
    return f'i_{index_name.value}'

def sma_cross_times(fast, slow, master, period):
    return f'sma_x_{fast}_{slow}_{master}_{period}'

def one_day_mom_surge(surge_percent):
    return f'1d_mom_surge_{surge_percent}'

def consecutive_days_above_threshold(days, threshold):
    return f'{days}_days_above_{threshold}'