from zipbird.position_manager.rotation_position_sizer import RotationPositionSizer
from zipbird.position_manager.atr_position_sizer import ATRPositionSizer
from zipbird.strategy.strategy_executor import StrategyExecutor

from zipbird.strategies.s1_weekly_rotation import S1WeeklyRotationStrategy
from zipbird.strategies.s2_mean_reversion_long_strategy import S2MRLong
from zipbird.strategies.s3_mean_reversion_short_strategy import S3MRShort


PARAMS_S1_WEEKLY_SP500 = dict(
    market_filter_sma_period=200,
    market_filter_tolerance=0.02,
    n_of_positions=10,
    
    max_rsi=50,
    rsi_len=3,
    roc_len=200,
    vol_window=50,
    rebalance_by_vol=False,

    use_spx=True,
    min_price=1.0,
    avg_volume_days=50,
    min_avg_dollar_volume=10_000_000,
    dollar_volume_rank_window=100,

    balance_weekday=0,  # Monday

)

PARAMS_S1_WEEKLY_1000 = PARAMS_S1_WEEKLY_SP500.copy()
PARAMS_S1_WEEKLY_1000['use_spx'] = False
PARAMS_S1_WEEKLY_1000['rebalance_by_vol'] = True

SE_S1_WEEKLY_ROTATION_SP500 = StrategyExecutor(
        strategy=S1WeeklyRotationStrategy(PARAMS_S1_WEEKLY_SP500), 
        position_sizer=RotationPositionSizer(PARAMS_S1_WEEKLY_SP500),
)

SE_S1_WEEKLY_ROTATION_1000 = StrategyExecutor(
        strategy=S1WeeklyRotationStrategy(PARAMS_S1_WEEKLY_1000), 
        position_sizer=RotationPositionSizer(PARAMS_S1_WEEKLY_1000),
)

PARAMS_S2_MR_LONG = dict(
    min_price=1.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    sma_period=150,
    rsi_period=3,
    atr_period=10,
    atr_percent_limit=4,  # 4%
    adx_period=7,
    rsi_lower_limit=0,
    rsi_upper_limit=30,
    adx_lower_limit=45,
    adx_upper_limit=100,
    
    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    open_position_factor=2,
    open_order_percent=0.04,
    stop_loss_atr_multiple=2.5,
    stop_loss_days=4,
    price_target_percent=0.03,
)

SE_S2_MR_LONG = StrategyExecutor(
        strategy=S2MRLong(PARAMS_S2_MR_LONG),
        position_sizer=ATRPositionSizer(PARAMS_S2_MR_LONG))

PARAMS_S3_MR_SHORT = dict(
    min_price=1.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    sma_period=150,
    rsi_period=3,
    atr_period=10,
    atr_percent_limit=4,  # 4%
    adx_period=7,
    rsi_lower_limit=0,
    rsi_upper_limit=30,
    adx_lower_limit=45,
    adx_upper_limit=100,
    days_up=3,
    
    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    open_position_factor=2,
    open_order_percent=0.04,
    stop_loss_atr_multiple=2.5,
    stop_loss_days=4,
    price_target_percent=0.03,
)

SE_S3_MR_SHORT = StrategyExecutor(
        strategy=S3MRShort(PARAMS_S3_MR_SHORT),
        position_sizer=ATRPositionSizer(PARAMS_S3_MR_SHORT))

STRATEGY_FUNC_MAP = {
    's1_sp500': SE_S1_WEEKLY_ROTATION_SP500,
    's1_1000': SE_S1_WEEKLY_ROTATION_1000,
    's2_mrlong': SE_S2_MR_LONG,
    's3_mrshort': SE_S3_MR_SHORT
}