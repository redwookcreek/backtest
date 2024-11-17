from zipbird.strategy.pipeline_maker import IndexNames, PipelineMaker


INDICATORS = (
    # SMA
    (PipelineMaker.add_sma, [20, 25, 50, 100, 150, 200]),
    # ATR
    (PipelineMaker.add_atr, [10, 20, 40]),
    (PipelineMaker.add_atrp, [10, 20, 40]),
    # RSI
    (PipelineMaker.add_rsi, [3, 4, 5, 7]),
    # ROC
    (PipelineMaker.add_roc, [3, 6, 50, 200]),
    # max in window
    (PipelineMaker.add_max_in_window, [20, 50, 100, 200]),
    # consecutive ups
    (PipelineMaker.add_consecutive_up, [3, 6]),
    # volitilty
    (PipelineMaker.add_vol_percentile, [100]),

)

DOLLAR_VOLUME_RANK_PERIOD = 200

class IndicatorLoader:
    def init(self, debug_logger):
        pass
    def get_order_fill_callback(self):
        return None
    
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        pipeline_maker.add_dollar_volume_rank(DOLLAR_VOLUME_RANK_PERIOD)
        pipeline_maker.add_index_consititue(IndexNames.SP500)
        for ind_func, periods in INDICATORS:
            binded_func = ind_func.__get__(pipeline_maker)
            for p in periods:
                binded_func(p)


        