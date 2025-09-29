# core/arch_client.py
import redis
import json
import pandas as pd
import logging
import os

class ArchClient:
    """Base class for clients. Subclasses must implement initialize and generate."""
    def __init__(self, config, client_name):  # Changed to client_name
        self.config = config
        self.client_name = client_name  # Changed from client_id
        self.redis = None  # Lazy init from previous patch
        self.pubsub = None
        self.context = {}  # Initialize context as empty dict
        self.initialize()  # Call subclass-specific initialization; context is populated here

    def _get_redis(self):
        if self.redis is None:
            self.redis = redis.Redis(host=self.config.get('redis_host', 'localhost'), port=self.config.get('redis_port', 6379), db=self.config.get('redis_db', 0))
        return self.redis

    def _make_json_serializable(self, obj):
        """Recursively convert non-JSON-serializable objects (e.g., Timestamp) to strings."""
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()  # Convert to ISO 8601 string
        elif isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        return obj

    def initialize(self):
        """Subclass-specific initialization (e.g., load params). Now sets up initial context."""
        self.context = {
            'current_date': None,
            'current_time': None,
            'current_market_data': None,
            # Additional custom info can be added here or in subclasses (e.g., from config)
            'custom_info': self.config.get('custom_client_info', {})  # Example: Pull from config
        }
        logging.info(f"Initialized context for {self.client_name}: {self.context}")

    def generate(self, data):
        raise NotImplementedError("Subclasses must implement generate() and return a pd.DataFrame")

    def push(self, period_start, outputs_df):
        """Push outputs to configured output. Expects outputs_df as pd.DataFrame."""
        output_type = self.config.get('output_type', 'parquet')  # Default to parquet
        
        if output_type == 'redis':
            # Serialize DataFrame to dict for Redis
            serializable_outputs = self._make_json_serializable(outputs_df.to_dict(orient='records'))
            redis_conn = self._get_redis()
            redis_conn.lpush(f"outputs:{period_start.strftime('%Y-%m-%d_%H:%M')}:{self.client_name}", json.dumps(serializable_outputs))
        elif output_type == 'parquet':
            output_dir = self.config.get('output_dir', './outputs')
            os.makedirs(output_dir, exist_ok=True)
            parquet_file = f"{output_dir}/outputs_{self.client_name}_{self.config['region']}.parquet"
            
            # Append to Parquet file (requires fastparquet or pyarrow; install if needed)
            if os.path.exists(parquet_file):
                outputs_df.to_parquet(parquet_file, engine='fastparquet', append=True, index=False)
            else:
                outputs_df.to_parquet(parquet_file, engine='fastparquet', index=False)
        else:
            # Fallback to JSON: Serialize DataFrame to list of dicts
            output_dir = self.config.get('output_dir', './outputs')
            os.makedirs(output_dir, exist_ok=True)
            serializable_outputs = self._make_json_serializable(outputs_df.to_dict(orient='records'))
            with open(f"{output_dir}/outputs_{period_start.strftime('%Y-%m-%d_%H:%M')}_{self.client_name}.json", 'w') as f:
                json.dump(serializable_outputs, f)
        
        logging.info(f"Client {self.client_name} pushed outputs for {period_start}")

    def process_period(self, period_start, data):
        # Update context for this event/period (use pd.Timestamp)
        self.context['current_date'] = pd.to_datetime(period_start).date()
        self.context['current_time'] = pd.to_datetime(period_start).time()
        self.context['current_market_data'] = data  # Full data dict for the period
        logging.debug(f"Updated context for period {period_start}: {self.context}")
        
        outputs_df = self.generate(data)
        self.push(period_start, outputs_df)
        logging.info(f"Client {self.client_name} processed period {period_start} directly (replay mode)")

    def listen(self):
        region = self.config['region']
        redis_conn = self._get_redis()
        self.pubsub = redis_conn.pubsub()
        self.pubsub.psubscribe(**{f"data|region|{region}|period|*": self._handler})
        logging.info(f"Client {self.client_name} listening to Redis for region {region}...")
        self.pubsub.run_in_thread(sleep_time=0.001)

    def _handler(self, message):
        if message['type'] == 'pmessage':
            channel = message['channel'].decode()
            period_start_str = channel.split('|')[-1]  # Extract last part after 'period|'
            try:
                period_start = pd.to_datetime(period_start_str)
            except ValueError as e:
                logging.error(f"Failed to parse period timestamp '{period_start_str}': {e}")
                return  # Skip invalid messages
            
            data_raw = json.loads(message['data'].decode())
            data = {df_type: pd.DataFrame(records) for df_type, records in data_raw.items()}
            
            # Update context for this event/period (use pd.Timestamp)
            self.context['current_date'] = pd.to_datetime(period_start).date()
            self.context['current_time'] = pd.to_datetime(period_start).time()
            self.context['current_market_data'] = data
            logging.debug(f"Updated context for period {period_start}: {self.context}")
            
            outputs_df = self.generate(data)
            self.push(period_start, outputs_df)
