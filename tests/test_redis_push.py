# test_redis_push.py
import redis
import json
import pandas as pd
from datetime import date, time  # For type checking

# Redis connection details (match your config)
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Test parameters (adjust as needed)
REGION = 'amer'  # Match your client's region
PERIOD_TIMESTAMP = '20231001T00:00'  # Example timestamp for channel
DATAFRAME_TYPE = 'market_data'  # Match dataframe_types in config

def make_json_serializable(obj):
    """Recursively convert non-JSON-serializable objects (e.g., Timestamp, date, time) to strings."""
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()  # Convert to ISO 8601 string
    elif isinstance(obj, date):
        return obj.isoformat()  # Convert date to 'YYYY-MM-DD'
    elif isinstance(obj, time):
        return obj.isoformat()  # Convert time to 'HH:MM:SS'
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    return obj

# Simulate sample data (dict of DataFrames with your schema)
sample_data = {
    DATAFRAME_TYPE: pd.DataFrame({
        'refts': [pd.Timestamp('2023-10-01 00:00:00'), pd.Timestamp('2023-10-01 00:00:00')],
        'date': [pd.Timestamp('2023-10-01').date(), pd.Timestamp('2023-10-01').date()],
        'time': [pd.Timestamp('2023-10-01 00:00:00').time(), pd.Timestamp('2023-10-01 00:00:00').time()],
        'instrument_id': ['instrid1', 'instrid2'],
        'value1': [1, 3],
        'value2': [2, 3],
        'value3': [3, 2]
    })
}

# Serialize data to JSON (as the broadcaster does), making it serializable
serialized_data = json.dumps({df_type: make_json_serializable(df.to_dict(orient='records')) for df_type, df in sample_data.items()})

# Connect to Redis and publish
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
channel = f"data|region|{REGION}|period|{PERIOD_TIMESTAMP}"
r.publish(channel, serialized_data)

print(f"Published test data to channel: {channel}")
