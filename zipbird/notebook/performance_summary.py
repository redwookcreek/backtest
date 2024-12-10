import pandas as pd
import empyrical as em
from IPython.core.display import display, HTML
import norgatedata
import plotly.graph_objects as go
from datetime import timedelta
import norgatedata
import pyfolio as pf
import matplotlib.pyplot as plt
import io
import base64
import contextlib

from zipbird.replay.order_collector import OrderCollector
from zipbird.utils import utils

def _benchmark_returns(returns):
    benchmark_prices = norgatedata.price_timeseries(
        '$SPXTR',
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        start_date=returns.index.tolist()[0].strftime('%Y-01-01'),
        end_date=returns.index.tolist()[-1].strftime('%Y-12-31'),
        timeseriesformat='pandas-dataframe',
    )
    return em.simple_returns(benchmark_prices['Close'])

def _monthly_return_table_cell_class(month_ret):
    if month_ret >= 0.15:
        return 'table-cell-dark-green'
    elif month_ret >= 0.1:
        return 'table-cell-green'
    elif month_ret >= 0.05:
        return 'table-cell-low-green'
    elif month_ret <= -0.15:
        return 'table-cell-dark-red'
    elif month_ret <= -0.1:
        return 'table-cell-red'
    elif month_ret <= -0.05:
        return 'table-cell-low-red'
    else:
        return 'table-cell-yellow'

def monthly_map(returns):
    """Returns a HTML table that display monthly performance.
    
    Each row is a year, each cell is one month's performance
    """
    table = """
    <table class='table table-hover table-condensed table-striped'>
    <thead>
    <tr>
    <th style="text-align:right">Year</th>
    <th style="text-align:right">Jan</th>
    <th style="text-align:right">Feb</th>
    <th style="text-align:right">Mar</th>
    <th style="text-align:right">Apr</th>
    <th style="text-align:right">May</th>
    <th style="text-align:right">Jun</th>
    <th style="text-align:right">Jul</th>
    <th style="text-align:right">Aug</th>
    <th style="text-align:right">Sep</th>
    <th style="text-align:right">Oct</th>
    <th style="text-align:right">Nov</th>
    <th style="text-align:right">Dec</th>
    <th style="text-align:right">Year</th>
    <th style="text-align:right">Benchmark</th>
    </tr>
    </thead>
    <tbody>
    <tr>"""

    monthly_data = em.aggregate_returns(returns,'monthly')
    yearly_data = em.aggregate_returns(returns,'yearly')
    benchmark_data = em.aggregate_returns(_benchmark_returns(returns), 'yearly')
    first_year = True
    first_month = True
    yr = 0
    mnth = 0
    for m, val in monthly_data.items():
        yr = m[0]
        mnth = m[1]

        if(first_month):
            table += "<td align='right'><b>{}</b></td>\n".format(yr)
            first_month = False

        if(first_year): # pad empty months for first year if sim doesn't start in January
            first_year = False
            if(mnth > 1):
                for i in range(1, mnth):
                    table += "<td align='right'>-</td>\n"

        table += "<td align='right' class='{:s}'>{:+.1f}</td>\n".format(
            _monthly_return_table_cell_class(val), val * 100)

        if(mnth==12): # check for dec, add yearly
            table += "<td align='right'><b>{:+.1f}</b></td>\n".format(yearly_data[yr] * 100)            
            table += "<td align='right'><b>{:+.1f}</b></td>\n".format(benchmark_data[yr] * 100)
            table += '</tr>\n <tr> \n'
            first_month = True

    # add padding for empty months and last year's value
    if(mnth != 12):
        for i in range(mnth+1, 13):
            table += "<td align='right'>-</td>\n"
            if(i==12):
                table += "<td align='right'><b>{:+.1f}</b></td>\n".format(
                    yearly_data[yr] * 100
                )
                table += "<td align='right'><b>{:+.1f}</b></td>\n".format(
                    benchmark_data[yr] * 100)
                table += '</tr>\n <tr> \n'
    table += '</tr>\n </tbody> \n </table>'
    return table

def holding_period_map(returns):
    """Returns a HTML table that prints performance if starts from that year"""
    yr = em.aggregate_returns(returns, 'yearly')

    yr_start = 0
    
    table = "<table class='table table-hover table-condensed table-striped'>\n"
    table += "<tr><th>Years</th>\n"
    
    for i in range(len(yr)):
        table += "<th>{}</th>\n".format(i+1)
    table += "</tr>\n"

    for the_year, value in yr.items(): # Iterates years
        table += "<tr><th>{}</th>\n".format(the_year) # New table row
        
        for yrs_held in (range(1, len(yr)+1)): # Iterates yrs held 
            if yrs_held   <= len(yr[yr_start:yr_start + yrs_held]):
                ret = em.annual_return(yr[yr_start:yr_start + yrs_held], 'yearly' )
                table += "<td>{:+.0f}</td>\n".format(ret * 100)
        table += "</tr>"    
        yr_start+=1
    table += '</table>\n'
    return table


def monthly_map_and_holding(returns):
    """Displays two HTML tables for monthly performance and holding period analysis
    
    Must be run inside a notebook
    """
    display(HTML('<h1>Monthly Performance</h1>'))
    display(HTML(monthly_map(returns)))

    display(HTML('<h1>Holding Period Analysis</h1>'))
    display(HTML(holding_period_map(returns)))


def print_correlation_mat(*all_returns):
    for i in range(len(all_returns)):
        print(
            ' '.join(
            '{:6.2f}'.format(all_returns[i].corr(all_returns[j]))
            for j in range(len(all_returns))))
        

# deprecated
def _make_round_trip(open_trade, close_trade):
    return {
        'sid': open_trade['sid'],
        'open_date': open_trade['dt'],
        'close_date': close_trade['dt'],
        'open_price': open_trade['price'],
        'close_price': close_trade['price'],
        'amount': open_trade['amount'],
        'close_amount': close_trade['amount'],
        'profit': (close_trade['price'] - open_trade['price']) * open_trade['amount'],
        'profit_percent': close_trade['price'] / open_trade['price'] - 1,
    }

# deprecated
def pair_round_trips(df):
    """Group the transactions into round trips
    
    This assumes no scaling in/out of the trades. All trades
    are done in one open trade and one close trade
    """
    # Sort the dataframe
    df = df.sort_values(['sid', 'dt'])
    
    round_trips = []
    
    for sid, group in df.groupby('sid'):
        open_trade = None
        
        for _, row in group.iterrows():
            if open_trade is None:
                open_trade = row
            else:  # Sell
                round_trips.append(_make_round_trip(open_trade, row))
                open_trade = None
    return pd.DataFrame(round_trips).sort_values(by='close_date')


def get_ployly_fig(round_trip_row):
    """Plot a OHLC chart for the ticker
    
    draw a up/down arrow at start_date, end_date
    amount is the open order amount, positive for long, negative for short

    Use the following in notebook to go through round trips

cur_trip = round_trips.iloc[cur_loc]
fig, ticker, start, end = get_ployly_fig(cur_trip)

print(cur_trip)
cur_loc += 1
fig.show()

# display the traded stock in amibroker
!C:\\Windows\\SysWOW64\\cscript.exe \\
  C:\\Users\liu_w\\OneDrive\\Documents\\zipline\\system-test\\script\\amibroker.js \\
      "$ticker" $start $end
    """
    ticker = round_trip_row['sid'].symbol if 'sid' in round_trip_row else round_trip_row['stock'].symbol
    start_date = round_trip_row['open_date']
    end_date = round_trip_row['close_date']
    
    # draw extra 40 days before and after the start/end date
    MARGIN = timedelta(days=40)
    DATE_FORMAT = '%Y-%m-%d'
    display_start = (start_date - MARGIN).strftime(DATE_FORMAT)
    display_end = (end_date + MARGIN).strftime(DATE_FORMAT)
    start_date_str = start_date.strftime(DATE_FORMAT)
    end_date_str = end_date.strftime(DATE_FORMAT)
    pricedata = norgatedata.price_timeseries(
        ticker,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        start_date=display_start,
        end_date=display_end,
        timeseriesformat='numpy-ndarray',
    )
    # find y position to plot the open/close point
    # plot above high for sell, plot below low for buy
    open_y = close_y = None
    is_long = round_trip_row['amount'] > 0

    for p in pricedata:        
        if pd.to_datetime(p[0]).strftime(DATE_FORMAT) == start_date_str:
            open_y = p[3] * 0.98 if is_long else p[2] * 1.02
        if pd.to_datetime(p[0]).strftime(DATE_FORMAT) == end_date_str:
            close_y = p[2] * 1.02 if is_long else p[3] * 0.98
    
    fig = go.Figure(data=go.Ohlc(
            x=[p[0] for p in pricedata],
            open=[p[1] for p in pricedata],
            high=[p[2] for p in pricedata],
            low=[p[3] for p in pricedata],
            close=[p[4] for p in pricedata],
        ))
    fig.update_layout(
        title=dict(
            text=(f'{ticker} {start_date_str} - {end_date_str} ({round_trip_row["trade_day"]} days) '
                  f'({round_trip_row["profit_percent"]*100:.2f}%)'),
            font=dict(size=20),
            automargin=True,
            yref='paper',
            xanchor='center',
            x=0.5,
        )
    )
    fig.add_annotation(
        x=start_date_str,
        y=open_y,
        text='{:.2f}'.format(round_trip_row['open_price']),
        showarrow=True,
        valign='bottom',
        ay=20 if is_long else -20,
        arrowhead=1)
    fig.add_annotation(
        x=end_date_str,
        y=close_y,
        text='{:.2f} ({:.2f}%)'.format(
            round_trip_row['close_price'],
            round_trip_row['profit_percent'] * 100),
        showarrow=True,
        ay=-20 if is_long else 20,
        arrowhead=1)
    return fig, ticker, display_start, display_end

_TABLE_CLASSES = "table table-striped table-bordered"

def save_pyfolio_tearsheet_with_text(returns, positions, transactions, f):
    
    # overall stats
    perf_stats = pf.plotting.show_perf_stats(
        returns,
        None, #_benchmark_returns(returns),
        positions=positions,
        transactions=transactions,
        return_df=True
    )
    f.write(perf_stats.to_html(classes=_TABLE_CLASSES))

    # drawdown periods
    drawdown_periods = pf.timeseries.gen_drawdown_table(returns, top=5)
    f.write(drawdown_periods.to_html(classes=_TABLE_CLASSES))    
            
    # rolling returns
    f.write(_create_figure_from_ax(
        pf.plotting.plot_rolling_returns(
            returns,
            logy=True),
        title='Cumulative returns on logarithmic scale'))

    # daily returns
    f.write(_create_figure_from_ax(
        pf.plotting.plot_returns(returns),
        title='Returns'))
    
    # rolling vol
    f.write(_create_figure_from_ax(
        pf.plotting.plot_rolling_volatility(returns)
    ))

    # rolling sharpe
    f.write(_create_figure_from_ax(
        pf.plotting.plot_rolling_sharpe(returns)
    ))

    # Drawdowns
    f.write(_create_figure_from_ax(
        pf.plotting.plot_drawdown_periods(returns, top=5)
    ))

    # underwater
    f.write(_create_figure_from_ax(
        pf.plotting.plot_drawdown_underwater(returns=returns)
    ))

    f.write(_create_figure_from_ax(
        pf.plotting.plot_annual_returns(returns)
    ))

    f.write(_create_figure_from_ax(
        pf.plotting.plot_monthly_returns_dist(returns)
    ))

    # gross leverage
    f.write(_create_figure_from_ax(pf.plot_gross_leverage(returns, positions)))

        
def _create_figure_from_ax(ax, title=None):
    if title:
        ax.set_title(title)
    fig = ax.get_figure()
    fig.set_size_inches(10, 6)
    
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    plt.close()
    buf.seek(0)
    string = base64.b64encode(buf.read()).decode('utf-8')
    return f'<img src="data:image/png;base64,{string}" />'

        


def performance_filename(prefix:str, start_date:pd.Timestamp, end_date:pd.Timestamp, label:str=''):
    return f'results/perf-{utils.filename(prefix, start_date, end_date, label)}.html'

def dict_to_html_table(dictionary):
    html = f'<table border="1" class={_TABLE_CLASSES}>\n'
    for key, value in dictionary.items():
        html += f'  <tr>\n    <td>{key}</td>\n    <td>{value}</td>\n  </tr>\n'
    html += '</table>'
    return html

_PRFORMANCE_HTML_CSS = """
/* Classes for positive returns */
td.table-cell-dark-green {
    background-color: #006400; /* Dark green */
    color: #fff;
}

td.table-cell-green {
    background-color: #228B22; /* Medium green */
    color: #fff;
}

td.table-cell-low-green {
    background-color: #b7e689; /* Light green */
    color: #000; /* Adjust text color for contrast */
}

/* Classes for negative returns */
td.table-cell-dark-red {
    background-color: #8B0000; /* Dark red */
    color: #fff;
}

td.table-cell-red {
    background-color: #FF4500; /* Medium red */
    color: #fff;
}

td.table-cell-low-red {
    background-color: #FFA07A; /* Light red */
    color: #000; /* Adjust text color for contrast */
}

/* Class for near-zero returns */
//td.table-cell-yellow {
//    background-color: #FFD700; /* Yellow */
//    color: #000;
//}
"""

def output_performance(
        prefix:str,
        start_date:pd.Timestamp,
        end_date:pd.Timestamp,
        strategy_name:str,
        strategy_params:dict,
        perf,
        label:str,
        bundle:str,
        replay_orders:OrderCollector):
    filename = performance_filename(prefix, start_date, end_date, label)
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(perf)
    maxdd, ann_ret = utils.get_main_perf(perf)

    with open(filename, 'w') as f:
        f.write(f'''
                <!DOCTYPE html>
                <html>
                <head>
                    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
                </head>
                <style>
                {_PRFORMANCE_HTML_CSS}
                </style>
                <body>
                ''')
        f.write(f'<h1>{strategy_name} - {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")} {label}</h1>')
        f.write(f'<p>Annual Return: {ann_ret:.2%}, Max Drawdown: {maxdd:.2%}</p>')
        if strategy_params:
            f.write('<p>Params</p>')
            params = strategy_params.copy()
            params['bundle'] = bundle
            f.write(dict_to_html_table(params))

        f.write('<h2>Monthly Performance</h2>\n')
        f.write(monthly_map(returns))

        f.write('<h2>Holding Period Analysis</h2>\n')
        f.write(holding_period_map(returns))

        f.write('<h2>Win Loss Stats By Year</h2>')
        f.write(win_rate_stats(replay_orders).to_html(classes="table table-striped table-bordered"))

        f.write('<h2>Tear sheet</h2>')
        save_pyfolio_tearsheet_with_text(returns, positions, transactions, f)
        f.write('</body>')

def win_rate_stats(trades:OrderCollector):
    df = trades.to_dataframe()
    df['is_win'] = df['profit_pct'] > 0
    df['is_loss'] = df['profit_pct'] < 0
    df['is_big_loss'] = df['profit_pct'] < -0.25
    df['is_big_win'] = df['profit_pct'] > .50

    yearly_stats = df.groupby('year').agg({
        'symbol': 'count',  # Count number of trades
        'profit_pct': [
            ('mean', 'mean'),
            ('std', 'std')
        ],
        'trade_days': ['mean'],
        'is_win': 'sum',  # Count of winning trades
        'is_loss': 'sum',  # Count of losing trades
        'is_big_loss': 'sum',
        'is_big_win': 'sum',
    })
    # Flatten column names
    yearly_stats.columns = ['trade_count', 
                            'profit_pct_avg',
                            'profit_pct_std',
                            'avg_trade_day', 
                            'win_count',
                            'loss_count',
                            'big_loss_count',
                            'big_win_count',]


    # Calculate average win and average loss
    yearly_stats['avg_win'] = df[df['profit_pct'] > 0].groupby('year')['profit_pct'].mean() * 100
    yearly_stats['avg_loss'] = df[df['profit_pct'] < 0].groupby('year')['profit_pct'].mean() * 100

    # Calculate win rate
    yearly_stats['win_rate'] = yearly_stats['win_count'] / yearly_stats['trade_count'] * 100
    yearly_stats['big_loss_rate'] = yearly_stats['big_loss_count'] / yearly_stats['trade_count'] * 100
    yearly_stats['big_win_rate'] = yearly_stats['big_win_count'] / yearly_stats['trade_count'] * 100

    yearly_stats = yearly_stats.sort_index()

    # Compute overall stats for all years
    overall_stats = pd.DataFrame({
        'trade_count': [df['symbol'].count()],
        'profit_pct_avg': [df['profit_pct'].mean()],
        'profit_pct_std': [df['profit_pct'].std()],
        'avg_trade_day': [df['trade_days'].mean()],
        'win_count': [df['is_win'].sum()],
        'loss_count': [df['is_loss'].sum()],
        'big_loss_count': [df['is_big_loss'].sum()],
        'avg_win': [df[df['profit_pct'] > 0]['profit_pct'].mean() * 100],
        'avg_loss': [df[df['profit_pct'] < 0]['profit_pct'].mean() * 100],
        'win_rate': [df['is_win'].sum() / df['symbol'].count() * 100],
        'big_loss_rate': [df['is_big_loss'].sum() / df['symbol'].count() * 100],
        'big_win_rate': [df['is_big_win'].sum() / df['symbol'].count() * 100],
    }, index=['All Years'])

    yearly_stats = pd.concat([yearly_stats, overall_stats])

    # Fill NaN values with 0 for years where there might be no wins or no losses
    yearly_stats = yearly_stats.fillna(0)

    # Round all float columns to 2 decimal places
    yearly_stats = yearly_stats.round(2)

    return yearly_stats