# clients/client1.py
from core.arch_client import ArchClient
import pandas as pd
import logging

class Client1(ArchClient):  # Must inherit from ArchClient
    def initialize(self):
        # Example: Add custom info to context during initialization
        self.context['custom_strategy_param'] = 2.0  # Example custom addition
        logging.info(f"Initialized Client1 with custom context: {self.context}")

    def generate(self, data):
        # Example: Use context in signal generation (e.g., filter based on current_date)
        current_date = self.context['current_date']
        logging.debug(f"Generating signals for date {current_date} using market data: {self.context['current_market_data']}")
        
        # Process data to return a pd.DataFrame
        signals_df = pd.DataFrame()  # Initialize empty DataFrame
        for df_type, df in data.items():
            if 'value1' in df.columns:
                df['signal'] = df['value1'] * self.context.get('custom_strategy_param', 1.0)  # Use context param
                signals_df = pd.concat([signals_df, df])  # Append to result DataFrame
        logging.info("Finish wave")
        return signals_df.reset_index(drop=True)
