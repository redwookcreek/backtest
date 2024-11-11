from enum import Enum
import pandas as pd

from zipline.pipeline import Pipeline
from zipline.pipeline.factors import Factor
from zipline.pipeline.filters import Filter
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import basic as basic_factors
from zipline_norgatedata.pipelines import NorgateDataUnadjustedClose
from zipline_norgatedata.pipelines import NorgateDataIndexConstituent

from zipbird.utils import factor_utils
from zipbird.strategy import pipeline_column_names as col_name

EXTRA_LENGTH = 100

class IndexNames(Enum):
    SP500 = 'S&P 500'

class PipelineMaker:
    """Pipeline maker.
    
    This will be a singleton. All strategies push their desired indicators
    to this pipeline maker.
    """
    def __init__(self):
        self.columns = {
            # all strategies need yesterday's close
            'close': USEquityPricing.close.latest,
        }
        self.universe = None
        self.filters = {}

    def make_pipeline(self) -> Pipeline:
        # There is no screen, each strategy must use their own screen method
        return Pipeline(columns=self.columns)
    
    def _maybe_add_column(self, name:str, factor:Factor):
        if name not in self.columns:
            self.columns[name] = factor
        return self.columns[name]
    
    def add_rsi(self, period):
        return self._maybe_add_column(
            name=col_name.rsi_name(period),
            factor=factor_utils.RSIFactor(
                    rsi_len=period,
                    window_length=period + EXTRA_LENGTH))
    
    def add_sma(self, period):
        return self._maybe_add_column(
            name=col_name.sma_name(period),
            factor= basic_factors.SimpleMovingAverage(
                inputs=[USEquityPricing.close],
                window_length=period))
    
    def add_atr(self, period):
        return self._maybe_add_column(
            name=col_name.atr_name(period),
            factor=factor_utils.ATRFactor(
                atr_len=period,
                window_length=period + EXTRA_LENGTH))
    
    def add_atrp(self, period):
        return self._maybe_add_column(
            name=col_name.atrp_name(period),
            factor=factor_utils.ATRPFactor(
                atr_len=period,
                window_length=period + EXTRA_LENGTH))
    
    def add_adx(self, period):
        return self._maybe_add_column(
            name=col_name.adx_name(period),
            factor=factor_utils.ADXFactor(
                adx_len=period,
                window_length=period + EXTRA_LENGTH))
    
    def add_vol(self, period):
        return self._maybe_add_column(
            name=col_name.vol_name(period),
            factor=factor_utils.StdFactorPercent(window_length=period))
    
    def add_max_in_window(self, period):
        return self._maybe_add_column(
            name=col_name.max_in_window(period),
            factor=factor_utils.MaxInWindowFactor(window_length=period))
    
    def add_vol_percentile(self, period):
        return self._maybe_add_column(
            name=col_name.vol_percentile_name(period),
            factor=factor_utils.StdPercentileFactor(window_length=period))

    def add_roc(self, period):
        return self._maybe_add_column(
            name=col_name.roc_name(period),
            factor=factor_utils.ROCFactor(window_length=period + EXTRA_LENGTH,
                                          roc_len=period))
    
    def add_consecutive_up(self, period):
        return self._maybe_add_column(
            name=col_name.consecutive_up_name(period),
            factor=factor_utils.ConsecutiveUpFactor(window_length=period)
        )

    def add_dollar_volume_rank(self, period):
        self._maybe_add_column(
            name=col_name.dollar_volume_rank(period),
            filter=factor_utils.DollarVolumeRankFactor(window_length=period)
        )
        
    def add_dollar_volume_rank_universe(self, max_rank:int, min_close:float, window_length:int):
        unadjusted_close = NorgateDataUnadjustedClose()
        dollar_volume_rank = factor_utils.DollarVolumeRankFactor(
            window_length=window_length,
        )
        universe_screen = (
                (unadjusted_close > min_close) & 
                (dollar_volume_rank <= max_rank)
        )
        self.add_universe(universe_screen)

    def add_universe(self, universe_screen):
        self.universe = universe_screen

    FILTER_NAME = 'universe'
    def add_filter(self, filter:Filter, filter_name:str=FILTER_NAME):
        if filter_name in self.columns:
            raise ValueError(f'duplicate filter name {filter_name}')
        self.columns[filter_name] = filter
        self.filters[filter_name] = filter

    def get_data_after_filter(self, pipeline_data:pd.DataFrame):
        to_return = pipeline_data
        for filter_name in self.filters:
            to_return = to_return[to_return[filter_name]]
        return to_return

    def add_sma_cross(self, period):
        return self._maybe_add_column(
            name=col_name.sma_cross(period),
            factor=factor_utils.SMACrossOver(
                sma_len=period, window_length=period+10),
            mask=self.universe
        )
    
    def add_sma_trend(self, period):
        return self._maybe_add_column(
            name=col_name.sma_trend(period),
            factor=factor_utils.SMATrend(sma_len=period, window_length=period+10),
            mask=self.universe
        )
    
    def add_index_consititue(self, index_name:IndexNames):
        match index_name:
            case IndexNames.SP500:
                return self._maybe_add_column(
                    name=index_name.value,
                    factor=NorgateDataIndexConstituent('S&P 500')
                )
        raise ValueError(f'Unhanlded index consititue {index_name}')
        