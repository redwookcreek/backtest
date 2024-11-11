import pandas as pd
from zipbird.strategy.strategy import BaseStrategy
from zipbird.strategy.pipeline_maker import PipelineMaker

class PipelineSaver:

    def __init__(self, strategies:list[BaseStrategy]):
        self.strategies = strategies
        self.pipeline_maker = PipelineMaker()

    def make_pipeline(self):
        for strategy in self.strategies:
            strategy.prepare_pipeline_columns(self.pipeline_maker)
        return self.pipeline_maker.make_pipeline()

    def record_pipeline_data(self, trade_day:pd.Timestamp, pipeline_data:pd.DataFrame):
        pass