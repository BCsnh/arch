# core/arch_data_loader.py
import pandas as pd
import requests
import logging
from dateutil.relativedelta import relativedelta
import warnings

class ArchDataLoader:
    def __init__(self, config):
        self.config = config

    def load_data(self, period_start, period_end):
        """Load dict of DataFrames for the period and region."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # Ensure period_start and period_end are pd.Timestamp
            period_start = pd.to_datetime(period_start)
            period_end = pd.to_datetime(period_end)
            
            if self.config['mode'] == 'live':
                return self._fetch_live_data(period_start, period_end)
            else:
                return self._load_historical_data(period_start, period_end)

    def _fetch_live_data(self, period_start, period_end):
        data = {}
        region = self.config['region']
        dataframe_types = self.config.get('dataframe_types', ['market_data'])
        for df_type in dataframe_types:
            # Placeholder: Fetch from API (customize for real source)
            # Simulate data with schema: datetime, date, time, instrument_id, value1, value2, value3
            # Dynamically generate for example instruments (no config-specified assets)
            example_instruments = ['instrid1', 'instrid2', 'instrid3']  # Dynamic; replace with real API fetch
            df = pd.DataFrame({
                'datetime': [period_start] * len(example_instruments),
                'date': [period_start.date()] * len(example_instruments),
                'time': [period_start.time()] * len(example_instruments),
                'instrument_id': example_instruments,
                'value1': [1, 3, 10],
                'value2': [2, 3, 22],
                'value3': [3, 2, 33]
            })
            # In real impl: Fetch from API, filter by region/period if needed
            data[df_type] = df
            logging.info(f"Loaded live {df_type} for region {region}")
        return data

    def _load_historical_data(self, period_start, period_end):
        data = {}
        region = self.config['region']
        dataframe_types = self.config.get('dataframe_types', ['market_data'])
        for df_type in dataframe_types:
            try:
                file_path = f"{self.config['historical_dir']}/{region}/{df_type}.csv"
                df = pd.read_csv(file_path, parse_dates=['datetime', 'date', 'time'])
                # Ensure period_start/end are Timestamp for comparison
                period_start = pd.to_datetime(period_start)
                period_end = pd.to_datetime(period_end)
                mask = (df['datetime'] >= period_start) & (df['datetime'] < period_end)
                data[df_type] = df.loc[mask].reset_index(drop=True)
                logging.info(f"Loaded historical {df_type} from {file_path} for region {region}")
            except Exception as e:
                logging.error(f"Error loading historical {df_type} for region {region}: {e}")
        return data

    def get_periods(self):
        try:
            start_val = self.config['historical_range']['start']
            end_val = self.config['historical_range']['end']
            logging.debug(f"Parsing historical_range: start={start_val} (type={type(start_val)}), end={end_val} (type={type(end_val)})")

            # Use pd.to_datetime to handle various formats and types (strings, dates, etc.)
            current = pd.to_datetime(start_val)
            end = pd.to_datetime(end_val)
        except KeyError as e:
            logging.error(f"Missing config key: {e}")
            raise ValueError(f"Config missing required key: {e}")
        except ValueError as e:
            logging.error(f"Invalid value in historical_range: {e}")
            raise ValueError("Invalid value in historical_range (ensure start/end are valid dates)")

        # Renamed from 'interval' to 'frequency'
        frequency = self.config.get('frequency', 'day')
        if 'frequency' not in self.config:
            logging.warning("'frequency' not found in config; defaulting to 'day'")

        periods = []
        while current < end:
            if frequency == 'minute':
                next_period = current + pd.Timedelta(minutes=1)
            elif frequency == 'day':
                next_period = current + pd.Timedelta(days=1)
            else:  # month
                next_period = current + relativedelta(months=1)
            periods.append((current, min(next_period, end)))
            current = next_period
        return periods
