from zipbird.position_manager.rotation_position_sizer import RotationPositionSizer
from zipbird.position_manager.atr_position_sizer import ATRPositionSizer
from zipbird.position_manager.split_target_position_sizer import SplitTargetPositionSizer
from zipbird.strategy.indicator_loader import IndicatorLoader
from zipbird.strategy.strategy_executor import StrategyExecutor

from zipbird.strategies.s1_weekly_rotation import S1WeeklyRotationStrategy
from zipbird.strategies.s2_mean_reversion_long_strategy import S2MRLong
from zipbird.strategies.s3_mean_reversion_short_strategy import S3MRShort
from zipbird.strategies.s21_long_momentume import S21LongMOM
from zipbird.strategies.s22_short_rsi_thrust import S22ShortRSIThrust
from zipbird.strategies.s23_long_mr import S23LongMR
from zipbird.strategies.s24_low_vol_long import S24LowVolLong
from zipbird.strategies.s25_adx_mr_long import S25ADXLongMR
from zipbird.strategies.s26_6day_surge_short import S26SixDaySurgeShort
from zipbird.strategies.s31_trend_50 import S31Trend50
from zipbird.strategies.s32_200_cross import S32Cross200MA

PARAMS_S1_WEEKLY_SP500 = dict(
    market_filter_sma_period=200,
    market_filter_tolerance=0.02,
    n_of_positions=10,
    
    balance_freq='weekly',
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

PARAMS_S1_MONTHLY_SP500 = PARAMS_S1_WEEKLY_SP500.copy()
PARAMS_S1_MONTHLY_SP500['balance_freq'] = 'monthly'

PARAMS_S1_QUARTERLY_SP500 = PARAMS_S1_WEEKLY_SP500.copy()
PARAMS_S1_QUARTERLY_SP500['balance_freq'] = 'quarterly'

PARAMS_S1_WEEKLY_1000 = PARAMS_S1_WEEKLY_SP500.copy()
PARAMS_S1_WEEKLY_1000['use_spx'] = False
PARAMS_S1_WEEKLY_1000['rebalance_by_vol'] = True

SE_S1_WEEKLY_ROTATION_SP500 = StrategyExecutor(
        strategy=S1WeeklyRotationStrategy('s1_sp500', PARAMS_S1_WEEKLY_SP500), 
        position_sizer=RotationPositionSizer(PARAMS_S1_WEEKLY_SP500),
)

SE_S1_MONTHLY_ROTATION_SP500 = StrategyExecutor(
        strategy=S1WeeklyRotationStrategy('s1_sp500_m', PARAMS_S1_MONTHLY_SP500), 
        position_sizer=RotationPositionSizer(PARAMS_S1_MONTHLY_SP500),
)

SE_S1_QUARTERLY_ROTATION_SP500 = StrategyExecutor(
        strategy=S1WeeklyRotationStrategy('s1_sp500_q', PARAMS_S1_QUARTERLY_SP500), 
        position_sizer=RotationPositionSizer(PARAMS_S1_QUARTERLY_SP500),
)

SE_S1_WEEKLY_ROTATION_1000 = StrategyExecutor(
        strategy=S1WeeklyRotationStrategy('s1_all', PARAMS_S1_WEEKLY_1000), 
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
        strategy=S2MRLong('s2', PARAMS_S2_MR_LONG),
        position_sizer=ATRPositionSizer(PARAMS_S2_MR_LONG))

PARAMS_S3_MR_SHORT = dict(
    min_price=5.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    rsi_period=3,
    atr_period=10,
    atr_percent_limit=5,  # 4%
    adx_period=7,
    rsi_lower_limit=85,
    rsi_upper_limit=100,
    adx_lower_limit=50,
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
        strategy=S3MRShort('s3', PARAMS_S3_MR_SHORT),
        position_sizer=ATRPositionSizer(PARAMS_S3_MR_SHORT))

PARAMS_S21_LONG_MOM = dict(
    min_price=5.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    spx_sma_period=100,
    fast_sma_period=25,
    slow_sma_period=50,
    roc_period=200,
    atr_period=20,
    
    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    stop_loss_atr_multiple=5,
    trailing_stop_percent=.25,
)

SE_S21_LONG_MOM = StrategyExecutor(
    strategy=S21LongMOM('s21', PARAMS_S21_LONG_MOM),
    position_sizer=ATRPositionSizer(PARAMS_S21_LONG_MOM),
)


PARAMS_S22_SHORT_RSI_THRUST = dict(
    min_price=5.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    rsi_period=3,
    atr_period=10,
    atr_percent_limit=3,  # 3%
    adx_period=7,
    rsi_lower_limit=90,
    rsi_upper_limit=100,
    days_up=3,
    
    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    open_position_factor=2,
    open_order_percent=0.04,
    stop_loss_atr_multiple=3,
    stop_loss_days=4,
    price_target_percent=0.04,
)

SE_S22_SHORT_RSI_THRUST = StrategyExecutor(
    strategy=S22ShortRSIThrust('s22', PARAMS_S22_SHORT_RSI_THRUST),
    position_sizer=ATRPositionSizer(PARAMS_S22_SHORT_RSI_THRUST),
)


PARAMS_S23_LONG_MR = dict(
    min_price=1.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    sma_period=150,
    roc_period=3,
    atr_period=10,
    last_3_day_drop=-.125,
    
    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    open_position_factor=2,
    open_order_percent=0.07,
    stop_loss_atr_multiple=2.5,
    stop_loss_days=4,
    price_target_percent=0.04,
)

SE_S23_LONG_MR = StrategyExecutor(
    strategy=S23LongMR('s23', PARAMS_S23_LONG_MR),
    position_sizer=ATRPositionSizer(PARAMS_S23_LONG_MR),
)

PARAMS_S24_LOW_VOL_LONG = dict(
    min_price=1.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    spx_sma_period=200,
    sma_period=200,
    vol_period=100,
    vol_percentile_low=10,
    vol_percentile_high=40,
    rsi_period=4,
    atr_period=40,


    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    stop_loss_atr_multiple=1.5,
    trailing_stop_percent=.20,
)

SE_S24_LOW_VOL_LONG = StrategyExecutor(
    strategy=S24LowVolLong('s24', PARAMS_S24_LOW_VOL_LONG),
    position_sizer=ATRPositionSizer(PARAMS_S24_LOW_VOL_LONG),
)


PARAMS_S25_ADX_LONG_MR = dict(
    min_price=1.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    sma_period=100,
    rsi_period=3,
    atr_period=10,
    atr_percent_limit=4,  # 4%
    adx_period=7,
    rsi_lower_limit=0,
    rsi_upper_limit=50,
    adx_lower_limit=50,
    
    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    open_position_factor=2,
    open_order_percent=0.03,
    stop_loss_atr_multiple=3,
    stop_loss_days=6,
    price_target_atr_multiple=1,
)
SE_S25_ADX_MR_LONG = StrategyExecutor(
    strategy=S25ADXLongMR('s26', PARAMS_S25_ADX_LONG_MR),
    position_sizer=ATRPositionSizer(PARAMS_S25_ADX_LONG_MR)
)

PARAMS_S26_6DAY_SURGE_SHORT = dict(
    min_price=5.0,
    dollar_volume_rank_window=100,
    dollar_volume_rank_max=1000,
    
    roc_period=6,
    atr_period=10,
    atr_percent_limit=3,  # 3%
    days_up=3,
    last_6day_min_up=0.2,  # rose at least 20% in last 6 days
    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    open_position_factor=2,
    open_order_percent=0.05,
    stop_loss_atr_multiple=3,
    stop_loss_days=3,
    price_target_percent=0.05,
)

SE_S26_6DAY_SURGE_SHORT = StrategyExecutor(
    strategy=S26SixDaySurgeShort('s26', PARAMS_S26_6DAY_SURGE_SHORT),
    position_sizer=ATRPositionSizer(PARAMS_S26_6DAY_SURGE_SHORT),
)

PARAMS_S31_TREND_50 = dict(
    min_price=1.0,
    avg_volume_days=20,
    min_avg_dollar_volume=20_000_000,
    
    spx_sma_period=50,
    spx_atr_threshold_multiple=2,
    atr_period=20,
    roc_period=50,

    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    stop_loss_atr_multiple=5,
    trailing_stop_percent=.175,
)

SE_S31_TREND_50 = StrategyExecutor(
    strategy=S31Trend50('s31', PARAMS_S31_TREND_50),
    position_sizer=ATRPositionSizer(PARAMS_S31_TREND_50),
)

PARAMS_S31_SPLIT = PARAMS_S31_TREND_50.copy()
PARAMS_S31_SPLIT['price_target_atr_multiple'] = 30

SE_S31_TREND_50_SPLIT = StrategyExecutor(
    strategy=S31Trend50('s31-split', PARAMS_S31_SPLIT),
    position_sizer=SplitTargetPositionSizer(PARAMS_S31_SPLIT),
)

PARAMS_S32_200_CROSS = dict(
    min_price=1.0,
    avg_volume_days=20,
    min_avg_dollar_volume=20_000_000,
    
    spx_sma_period=200,
    sma_period=200, 
    spx_atr_threshold_multiple=0,
    atr_period=20,
    roc_period=50,

    max_positions=10,
    fraction_risk=0.02,  # 2% risk per position
    max_equity_per_position = 0.1,  # max 10% equity per position
    stop_loss_atr_multiple=5,
    trailing_stop_percent=.175,
)

SE_S32_200_CROSS = StrategyExecutor(
    strategy=S32Cross200MA('s32', PARAMS_S32_200_CROSS),
    position_sizer=ATRPositionSizer(PARAMS_S32_200_CROSS),
)

PARAMS_S32_SPLIT = PARAMS_S32_200_CROSS.copy()
PARAMS_S32_SPLIT['price_target_atr_multiple'] = 15

SE_S32_200_CROSS_SPLIT = StrategyExecutor(
    strategy=S32Cross200MA('s32-split', PARAMS_S32_SPLIT),
    position_sizer=SplitTargetPositionSizer(PARAMS_S32_SPLIT),
)

STRATEGY_FUNC_MAP = {
    's1_sp500': SE_S1_WEEKLY_ROTATION_SP500,
    's1_sp500_m': SE_S1_MONTHLY_ROTATION_SP500,
    's1_sp500_q': SE_S1_QUARTERLY_ROTATION_SP500,
    's1_1000': SE_S1_WEEKLY_ROTATION_1000,
    's2_mrlong': SE_S2_MR_LONG,
    's3_mrshort': SE_S3_MR_SHORT,
    's21_longmom': SE_S21_LONG_MOM,
    's22_short_rsi_thrust': SE_S22_SHORT_RSI_THRUST,
    's23_long_mr': SE_S23_LONG_MR,
    's24_low_vol_long': SE_S24_LOW_VOL_LONG,
    's25_adx_mr_long': SE_S25_ADX_MR_LONG,
    's26_6day_surge_short': SE_S26_6DAY_SURGE_SHORT,
    's31_trend_50': SE_S31_TREND_50,
    's31_trend_50_split': SE_S31_TREND_50_SPLIT,
    's32_200_cross': SE_S32_200_CROSS,
    's32_200_cross_split': SE_S32_200_CROSS_SPLIT,
    'ind_loader': IndicatorLoader()
}