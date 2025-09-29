# core/arch_data_loader.py
import pandas as pd
import requests
import logging
from dateutil.relativedelta import relativedelta
import warnings
from .period_helper import get_periods_comprehensive

class ArchDataLoader:
    def __init__(self, config):
        self.config = config
        self.datasources = config.get('datasources', [])
        if 'market_data' not in self.datasources:
            raise ValueError("'datasources' must include 'market_data' as mandatory.")

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
        for ds in self.datasources:
            # Placeholder: Fetch from API (customize for real source)
            # Simulate data with schema: refts, date, time, instrument_id, value1, value2, value3
            # Dynamically generate for example instruments (no config-specified assets)
            example_instruments = ['instrid1', 'instrid2', 'instrid3']  # Dynamic; replace with real API fetch
            df = pd.DataFrame({
                'refts': [period_start] * len(example_instruments),
                'date': [period_start.date()] * len(example_instruments),
                'time': [period_start.time()] * len(example_instruments),
                'instrument_id': example_instruments,
                'value1': [1, 3, 10],
                'value2': [2, 3, 22],
                'value3': [3, 2, 33]
            })
            # In real impl: Fetch from API, filter by region/period if needed
            data[ds] = df
            logging.info(f"Loaded live {ds} for region {region}")
        return data

    def _load_historical_data(self, period_start, period_end):
        data = {}
        region = self.config['region']
        for ds in self.datasources:
            try:
                file_path = f"{self.config['historical_dir']}/{region}/{ds}.csv"
                df = pd.read_csv(file_path, parse_dates=['refts', 'date', 'time'])
                mask = (df['refts'] >= period_start) & (df['refts'] < period_end)  # Use 'refts' for slicing
                data[ds] = df.loc[mask].reset_index(drop=True)
                logging.info(f"Loaded historical {ds} from {file_path} for region {region}")
            except Exception as e:
                logging.error(f"Error loading historical {ds} for region {region}: {e}")
        return data

    def get_periods(self):
        config = {}
        config['start_date'] = self.config['historical_range']['start']
        config['end_date'] = self.config['historical_range']['end']
        if 'frequency' not in self.config:
            logging.warning("'frequency' not found in config; defaulting to 'day'")
        config['frequency'] = self.config.get('frequency', 'day')
        if 'calendar' not in self.config:
            logging.warning("'calendar' not found in config; defaulting to '24/5'")
        config['calendar'] = self.config.get('calendar', '24/5')
        if not config['frequency'] in ['day','month']:
            if 'open_time' not in self.config['historical_range']:
                logging.error("'open_time' not found in config['historical_range']")
            if 'close_time' not in self.config['historical_range']:
                logging.error("'close_time' not found in config['historical_range']")
        config['open_time'] = self.config['historical_range'].get('open_time', None)
        config['close_time'] = self.config['historical_range'].get('close_time', None)
        return get_periods_comprehensive(config)
        

