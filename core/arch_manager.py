# core/arch_manager.py
import schedule
import time
import logging
import pandas as pd
from dateutil.relativedelta import relativedelta
from .arch_data_loader import ArchDataLoader
from .arch_broadcaster import ArchBroadcaster

class ArchManager:
    def __init__(self, config):
        self.config = config
        self.loader = ArchDataLoader(config)
        self.broadcaster = ArchBroadcaster(config)

    def run_period_live(self):
        now = pd.Timestamp.now()  # Use pd.Timestamp for consistency
        frequency = self.config['frequency']
        if frequency == 'minute':
            period_start = now.replace(second=0, microsecond=0)
            period_end = period_start + pd.Timedelta(minutes=1)
        elif frequency == 'day':
            period_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            period_end = period_start + pd.Timedelta(days=1)
        else:
            period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            period_end = period_start + relativedelta(months=1)
        
        data = self.loader.load_data(period_start, period_end)
        self.broadcaster.broadcast(period_start, data)

    def start(self):
        frequency = self.config['frequency']
        if frequency == 'minute':
            schedule.every(1).minutes.do(self.run_period_live)
        elif frequency == 'day':
            schedule.every().day.at("00:00").do(self.run_period_live)
        else:
            schedule.every(1).month.do(self.run_period_live)
        logging.info("Arch server running in live mode...")
        while True:
            schedule.run_pending()
            time.sleep(1)
