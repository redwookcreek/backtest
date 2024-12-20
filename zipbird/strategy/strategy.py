import pandas as pd

from zipline.protocol import Positions

from zipbird.basic.signal import Signal
from zipbird.basic.types import Equity
from zipbird.strategy.pipeline_maker import PipelineMaker


class BaseStrategy:
    """Base strategy. A strategy provides open/close signals"""
    def __init__(self, strategy_name, params):
        self.strategy_name = strategy_name
        self.params = params

    def get_name(self):
        return self.strategy_name
    
    def get_params(self):
        return self.params

    def make_pipeline(self, pipeline_maker:PipelineMaker) -> pd.DataFrame:
        pipeline_maker.add_dollar_volume_rank_universe(
            min_close=self.params['min_price'],
            max_rank=self.params['dollar_volume_rank_max'],
            window_length=self.params['dollar_volume_rank_window'],
        )
        filter = self.prepare_pipeline_columns(pipeline_maker)
        pipeline_maker.add_filter(filter=filter, filter_name='st_filter')

    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker) -> pd.DataFrame:
        """Create zipline pipeline columns"""

    def filter_pipeline_data(self, pipeline_data:pd.DataFrame) -> pd.DataFrame:
        """Returns data frame for filtered pipeline_data"""
        
    def generate_signals(self,
                         positions: Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        """Generate signals
        
        positions: current positions in portfolio
        pipeline_data: raw pipeline data
        filtered_pipeline_data: pipeline data after filtering defined in prepare_pipeline_columns
        """
        
    @classmethod
    def get_buy_list(cls,
                     buy_list: list[Equity],
                     positions:Positions,
                     max_positions,
                     open_position_factor=1):
        buy_list = buy_list[~buy_list.index.isin(positions.keys())]
        n_pos_to_open = max(0, (max_positions - len(positions)) * open_position_factor)
        return buy_list.index.tolist()[:n_pos_to_open]

