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
