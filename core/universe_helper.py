import pandas as pd

def get_universe(univ, period_start, period_end):
    # Dynamically generate for example instruments (no config-specified assets)
    period_start = pd.to_datetime(period_start)
    period_end = pd.to_datetime(period_end)
    example_instruments = ['instrid1', 'instrid2', 'instrid2', 'instrid3']  # Dynamic; replace with real API fetch
    example_dts = ['20230103','20230103','20230104','20230104']
    df = pd.DataFrame({
        'refts': pd.to_datetime(example_dts),
        'date': pd.to_datetime(example_dts),
        'time': [pd.to_datetime('19700101').time()] * 4,
        'instrument_id': example_instruments,
        'value1': [1, 3, 10, 23],
        'value2': [2, 3, 22, 9342],
        'value3': [3, 2, 33, 123]
        })
    df['in_universe'] = 1
    return df[['date','refts','instrument_id','in_universe']]
