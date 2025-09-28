# core/arch_broadcaster.py
import redis
import json
import logging
import os

class ArchBroadcaster:
    def __init__(self, config):
        self.config = config
        self.redis = redis.Redis(host=config['redis_host'], port=config['redis_port'], db=config['redis_db'])

    def broadcast(self, period_start, data):
        region = self.config['region']
        # Use specified format with '|' and compact timestamp
        timestamp_str = period_start.strftime('%Y%m%dT%H%M')
        channel = f"data|region|{region}|period|{timestamp_str}"
        serialized_data = json.dumps({df_type: df.to_dict(orient='records') for df_type, df in data.items()})
        self.redis.publish(channel, serialized_data)
        logging.info(f"Broadcast data to channel {channel} for region {region}")

        # Archive to disk in live mode
        if self.config['mode'] == 'live':
            archive_dir = self.config.get('archive_dir', './archive')
            os.makedirs(archive_dir, exist_ok=True)
            archive_file = f"{archive_dir}/region_{region}_period_{timestamp_str}.json"
            with open(archive_file, 'w') as f:
                f.write(serialized_data)
            logging.info(f"Archived live data to {archive_file}")
