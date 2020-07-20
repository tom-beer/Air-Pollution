import pandas as pd
import datetime as dt
from functools import wraps


def log_step(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = dt.datetime.now()
        result = func(*args, **kwargs)
        time_taken = str(dt.datetime.now() - tic)
#         print(f"Ran step {func.__name__} shape={result.shape} took {time_taken}s")
        return result
    return wrapper


@log_step
def start_pipeline(dataf):
    return dataf.copy() 


@log_step
def rename_columns(dataf):
    return (dataf
            .rename(columns={dataf.columns[-1]: 'DateTime'})
            .rename(columns={colname: colname.strip() for colname in dataf.columns})
           )


@log_step
def remove_rows_columns(dataf):
    return (dataf
            .drop(columns=[dataf.columns[0]])  # first column is station name
            .drop([0])  # first row is units of measurement
            .iloc[:-8]  # last 8 rows are summary statistics
            )


@log_step
def set_dtypes(dataf):
    dataf['DateTime'] = dataf['DateTime'].apply(lambda x: x.strip().replace('24:00', '00:00'))
    dataf['DateTime'] = pd.to_datetime(dataf['DateTime'], infer_datetime_format=True)
    pollutants = list(dataf)
    pollutants.remove('DateTime')
    dataf[pollutants] = dataf[pollutants].apply(pd.to_numeric, errors='coerce').clip(0)
    dataf = dataf.set_index('DateTime')
    return dataf


@log_step
def trim_dataset(dataf, start_time, end_time):
    return dataf[start_time:end_time]


@log_step
def add_station_name_to_cols(dataf, station_name):
    return dataf.rename(columns={colname: station_name + "_" + colname for colname in dataf.columns})


@log_step
def handle_missing_values(dataf):
    # For now naively fill with 0; Later distinguish between cases of nearest neighbor imputation
    return dataf.fillna(0)
