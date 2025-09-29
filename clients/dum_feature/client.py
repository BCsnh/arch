# clients/client2.py
from core.arch_client import ArchClient
import pandas as pd
import logging

class Client2(ArchClient):  # Must inherit from ArchClient
    def initialize(self):
        # Example: Add custom info to context during initialization
        self.context['custom_threshold'] = 50  # Example custom addition
        logging.info(f"Initialized Client2 with custom context: {self.context}")

    def generate(self, data):
        # Example: Use context in signal generation (e.g., conditional based on current_time)
        current_time = self.context['current_time']
        logging.debug(f"Generating signals at time {current_time} using market data: {self.context['current_market_data']}")
        
        # Process data to return a pd.DataFrame
        signals_df = pd.DataFrame()  # Initialize empty DataFrame
        for df_type, df in data.items():
            if 'value2' in df.columns and 'value3' in df.columns:
                df['signal'] = df['value2'] + df['value3']
                df['above_threshold'] = df['signal'] > self.context.get('custom_threshold', 0)  # Use context param
                signals_df = pd.concat([signals_df, df])  # Append to result DataFrame
        return signals_df.reset_index(drop=True)
