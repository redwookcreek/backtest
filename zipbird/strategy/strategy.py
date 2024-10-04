import pandas as pd

from zipline.protocol import Positions

from zipbird.basic.signal import Signal
from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.position_manager.position_sizer import PositionSizer


class BaseStrategy:
    """Base strategy. A strategy provides open/close signals"""
    def __init__(self, params):
        self.params = params

    def get_params(self):
        return self.params

    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker) -> pd.DataFrame:
        """Create zipline pipeline"""

    def generate_signals(self,
                         positions: Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        """Generate signals
        
        positions: current positions in portfolio
        pipeline_data: raw pipeline data
        filtered_pipeline_data: pipeline data after filtering defined in prepare_pipeline_columns
        """

