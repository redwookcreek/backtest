import pandas as pd
import empyrical as em
from IPython.core.display import display, HTML
import norgatedata
import plotly.graph_objects as go
from datetime import timedelta
import norgatedata


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


    benchmark_prices = norgatedata.price_timeseries(
        '$SPXTR',
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        start_date=returns.index.tolist()[0].strftime('%Y-01-01'),
        end_date=returns.index.tolist()[-1].strftime('%Y-12-31'),
        timeseriesformat='pandas-dataframe',
    )
    monthly_data = em.aggregate_returns(returns,'monthly')
    yearly_data = em.aggregate_returns(returns,'yearly')
    benchmark_data = em.aggregate_returns(em.simple_returns(benchmark_prices['Close']), 'yearly')
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

        table += "<td align='right'>{:+.1f}</td>\n".format(val * 100)

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
    
    table = "<table class='table table-hover table-condensed table-striped'>"
    table += "<tr><th>Years</th>"
    
    for i in range(len(yr)):
        table += "<th>{}</th>".format(i+1)
    table += "</tr>"

    for the_year, value in yr.items(): # Iterates years
        table += "<tr><th>{}</th>".format(the_year) # New table row
        
        for yrs_held in (range(1, len(yr)+1)): # Iterates yrs held 
            if yrs_held   <= len(yr[yr_start:yr_start + yrs_held]):
                ret = em.annual_return(yr[yr_start:yr_start + yrs_held], 'yearly' )
                table += "<td>{:+.0f}</td>".format(ret * 100)
        table += "</tr>"    
        yr_start+=1
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
