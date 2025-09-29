import logging
import os
import importlib
from datetime import datetime
import pandas as pd
from functools import partial
import multiprocessing as mp
import traceback
from core.arch_data_loader import ArchDataLoader
from core.arch_calendar import Calendar
from core.arch_client import ArchClient

def process_period_sequential(loader, client, period_tuple):
    period_start, period_end = period_tuple
    logging.info(f"Sequential: Starting processing for period {period_start} to {period_end}")
    data = loader.load_data(period_start, period_end)
    logging.info(f"Sequential: Data loaded for period {period_start} to {period_end}")
    client.process_period(period_start, data)
    logging.info(f"Sequential: Finished processing for period {period_start} to {period_end}")

def process_period_parallel(config, client_script_abs, client_name, client_dir, period_tuple, sublog_dir, replay_run_name, run_timestamp):
    period_start, period_end = period_tuple
    period_start_str = period_start.strftime('%Y%m%d') if isinstance(period_start, (pd.Timestamp, datetime)) else str(period_start)
    pid = os.getpid()
    subjob_log_file = f"{sublog_dir}/{replay_run_name}_{run_timestamp}_{pid}_{period_start_str}.log"

    # Set up subjob-specific logger
    subjob_logger = logging.getLogger(f"subjob_{period_start_str}")
    subjob_logger.setLevel(logging.INFO)
    fh = logging.FileHandler(subjob_log_file)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(processName)s - %(message)s'))
    subjob_logger.addHandler(fh)

    try:
        # Copy config to avoid issues
        config = config.copy()
        
        # Set output_dir
        config['output_dir'] = os.path.join(client_dir, config.get('output_dir', 'outputs'))
        
        # Recreate loader
        loader = ArchDataLoader(config)
        
        # Dynamically import client module
        spec = importlib.util.spec_from_file_location("client_module", client_script_abs)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        client_class = next((cls for cls in vars(module).values() if isinstance(cls, type) and issubclass(cls, ArchClient) and cls != ArchClient), None)
        if not client_class:
            raise ValueError(f"No ArchClient subclass found in {client_script_abs}")
        
        # Create client instance
        client = client_class(config, client_name)
        
        # Process the period with logging
        subjob_logger.info(f"Parallel (PID {pid}): Starting processing for period {period_start} to {period_end}")
        data = loader.load_data(period_start, period_end)
        subjob_logger.info(f"Parallel (PID {pid}): Data loaded for period {period_start} to {period_end}")
        client.process_period(period_start, data)
        subjob_logger.info(f"Parallel (PID {pid}): Finished processing for period {period_start} to {period_end}")
        return period_tuple  # Return on success
    finally:
        subjob_logger.removeHandler(fh)
        fh.close()

def run_replay(config, is_parallel, client_class, client_script_abs, client_dir, config_name, log_dir, run_timestamp):
    loader = ArchDataLoader(config)

    # Use calendar if enabled in config
    if 'calendar' in config:
        calendar = Calendar(config)
        valid_dates = calendar.get_valid_dates(config['historical_range']['start'], config['historical_range']['end'])
        periods = [(date, date + pd.Timedelta(days=1)) for date in valid_dates]  # Example: daily periods on valid dates
    else:
        periods = loader.get_periods()  # Fallback to original sequential periods

    # Set replay output dir relative to client's folder path
    config['output_dir'] = os.path.join(client_dir, config.get('output_dir', 'outputs'))  # Relative to client

    # Prepare for subjob logging and summary
    replay_run_name = f"{config['client_name']}_replay_{config_name}"
    start_date = pd.to_datetime(config['historical_range']['start']).strftime('%Y%m%d')
    end_date = pd.to_datetime(config['historical_range']['end']).strftime('%Y%m%d')
    successful_periods = []
    failed_periods = []

    if is_parallel:
        sublog_dir = f"{log_dir}/{replay_run_name}_{start_date}_{end_date}_parallel_{run_timestamp}"
        os.makedirs(sublog_dir, exist_ok=True)
        import multiprocessing_logging
        multiprocessing_logging.install_mp_handler()
        with mp.Pool(processes=4) as pool:
            tasks = []
            for period in periods:
                process_func = partial(process_period_parallel, config, client_script_abs, config['client_name'], client_dir, period, sublog_dir, replay_run_name, run_timestamp)
                tasks.append((period, pool.apply_async(process_func)))

            # Collect results, handling failures individually
            for period, res in tasks:
                try:
                    res.get(timeout=600)  # 10-min timeout; adjust as needed
                    successful_periods.append(period)
                except (mp.TimeoutError, Exception) as e:
                    failed_periods.append(period)
                    logging.exception(f"Failed to process period {period} (timeout or error): {e}\nTraceback: {traceback.format_exc()}")

            pool.close()
            pool.join()
    else:
        client = client_class(config, config['client_name'])
        for period in periods:
            try:
                process_period_sequential(loader, client, period)
                successful_periods.append(period)
            except Exception as e:
                failed_periods.append(period)
                logging.exception(f"Sequential: Failed to process period {period}: {e}\nTraceback: {traceback.format_exc()}")

    # Overall summary
    total_periods = len(periods)
    num_success = len(successful_periods)
    num_failed = len(failed_periods)
    if num_failed == 0:
        summary_msg = f"Replay completed successfully for client {config['client_name']}. All {total_periods} subjobs successful."
    else:
        failed_list = [str(p) for p in failed_periods]
        summary_msg = f"Replay completed for client {config['client_name']}. {num_success}/{total_periods} subjobs successful. Failed subjobs: {', '.join(failed_list)}"
    logging.info(summary_msg)
