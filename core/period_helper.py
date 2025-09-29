import pandas as pd
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas_market_calendars as mcal

def get_periods_comprehensive(config):
    start = pd.to_datetime(config['start_date'])
    end = pd.to_datetime(config['end_date'])

    periods = []
    frequency = config.get('frequency', 'day')
    if 'frequency' not in config:
        logging.warning("'frequency' not found in config; defaulting to 'day'")

    # Compute effective_end
    if end.time() == datetime.min.time():
        effective_end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    else:
        effective_end = end

    # Get trading calendar
    cal = mcal.get_calendar(config['calendar'])
    valid_days = cal.valid_days(start_date=start, end_date=end).normalize().tz_localize(None)

    if frequency == 'minute' or frequency.endswith('min'):
        if frequency == 'minute':
            interval = 1
        else:
            interval = int(frequency[:-3])

        # Expect open_time and close_time
        open_str = config['open_time']
        close_str = config['close_time']
        open_t = datetime.strptime(open_str, '%H:%M').time()
        close_t = datetime.strptime(close_str, '%H:%M').time()

        days = pd.date_range(start.floor('D'), end.floor('D'), freq='D')
        for day_dt in days:
            if day_dt not in valid_days:
                continue
            open_dt = pd.Timestamp.combine(day_dt.date(), open_t)
            close_dt = pd.Timestamp.combine(day_dt.date(), close_t)
            session_start = max(open_dt, start)
            session_end = min(close_dt, effective_end)
            if session_start >= session_end:
                continue
            current = session_start
            while current < session_end:
                next_period = current + pd.Timedelta(minutes=interval)
                period_end = min(next_period, session_end)
                if period_end < session_end:
                    period_end -= pd.Timedelta(microseconds=1)
                periods.append((current, period_end))
                current = next_period

    elif frequency == 'day':
        first_day = start.floor('D')
        last_day = effective_end.floor('D')
        days = pd.date_range(first_day, last_day, freq='D')
        for day in days:
            if day not in valid_days:
                continue
            period_start = max(start, day)
            period_end = min(effective_end, day + pd.Timedelta(days=1))
            if period_start >= period_end:
                continue
            if period_end == day + pd.Timedelta(days=1):
                period_end -= pd.Timedelta(microseconds=1)
            periods.append((period_start, period_end))

    else:  # month
        current = start
        while current < effective_end:
            first_of_current = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            next_period = first_of_current + relativedelta(months=1)
            period_end = min(next_period, effective_end)
            if period_end == next_period:
                period_end -= pd.Timedelta(microseconds=1)

            # Find trading days in [current, period_end]
            trading_days = valid_days[(valid_days >= current) & (valid_days <= period_end)]
            if len(trading_days) == 0:
                current = next_period
                continue

            first_trading = trading_days[0]
            last_trading = trading_days[-1]

            # Adjust actual_start
            if current.date() == first_trading.date():
                actual_start = current
            else:
                actual_start = first_trading.replace(hour=0, minute=0, second=0, microsecond=0)

            # Adjust actual_end
            end_of_last_trading = last_trading.replace(hour=23, minute=59, second=59, microsecond=999999)
            actual_end = min(period_end, end_of_last_trading)

            if actual_start < actual_end:
                periods.append((actual_start, actual_end))

            current = next_period

    return periods
