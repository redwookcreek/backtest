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

def sma_cross(period):
    return f'sma_cross_{period}'

def sma_trend(period):
    return f'sma_trend_{period}'

def dollar_volume_rank(period):
    return f'dv_rank_{period}'