# core/arch_calendar.py
import pandas as pd
import logging
from pandas_market_calendars import get_calendar  # Requires pip install pandas_market_calendars

class Calendar:
    """Base class for market calendars, wrapping pandas_market_calendars for extensibility."""
    def __init__(self, config):
        self.config = config
        self.exchange = config.get('calendar', 'NYSE')  # Default to NYSE; customizable
        logging.info(f"Initialized Calendar for exchange: {self.exchange}")

    def get_valid_dates(self, start, end):
        """Get list of valid trading dates between start and end."""
        cal = get_calendar(self.exchange)
        valid_dates = cal.valid_days(start_date=start, end_date=end)
        # original code, but mcal returns regular dates with UTC tz attached, doesn't seem to make sense for tz attachment
        # return [pd.to_datetime(date) for date in valid_dates]  # Return as list of pd.Timestamp
        # Now we convert to tz agnostic timesetamp for now
        return [pd.to_datetime(date).tz_localize(tz=None) for date in valid_dates]  # Return as list of pd.Timestamp

    # For customization/extension in subclasses
    def customize_dates(self, dates):
        """Override in subclasses for custom filtering (e.g., holidays, custom rules)."""
        return dates
