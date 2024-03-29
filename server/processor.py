import concurrent.futures
import io
import re

import numpy as np
import pandas as pd
from dateutil.parser import parse

def read_file(file):
    """
    Read file and return DataFrame based on file extension.
    Supports CSV and Excel formats.
    """
    # Read CSV file
    if file.name.endswith('.csv'):
        try:
            df = pd.read_csv(io.StringIO(file.read().decode('windows-1252')), encoding="windows-1252")
            return df
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {e}")

    # Read Excel file
    elif file.name.endswith('.xlsx') or file.name.endswith('.xls'):
        try:
            df = pd.read_excel(file)
            return df
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {e}")

    else:
        raise ValueError("Unsupported file format. Only CSV and Excel files are supported.")


def process_chunk(chunk):
    chunk = infer_and_convert_data_types(chunk)
    return {col: str(chunk[col].dtype) for col in chunk.columns}

# Function to parse dates
def parse_date(date_str):
    try:
        if isinstance(date_str, str):
            date = parse(date_str, dayfirst=True)
        else:
            date = date_str
        return date
    except ValueError:
        return None


def infer_date(col):
    # Identify and parse dates
    col = col.apply(parse_date)

    # Update column type to date
    col = pd.to_datetime(col)

    return col


def infer_numbers(col):
    col = pd.to_numeric(col, errors='coerce')
    return col


def to_complex(x):
    if isinstance(x, str):
        # Regular expression to extract real and imaginary parts
        parts = re.findall(r'[-+]?\d*\.\d+|\d+', x)
        if len(parts) == 2:
            return complex(float(parts[0]), float(parts[1]))
    return None


def infer_complex_nums(col):
    col = col.apply(to_complex)
    return col


def try_convert_to_timedelta(value):
    try:
        return pd.to_timedelta(value)
    except Exception:
        return pd.NaT


def infer_timedelta(col):
    col = col.apply(try_convert_to_timedelta)
    return col


def infer_boolean_column(col):
    # Define mapping of values
    mapping = {
        'true': True,
        'false': False,
        'yes': True,
        'no': False,
        '1': True,
        '0': False
    }

    # Convert values based on mapping
    col = col.apply(lambda x: mapping.get(str(x).lower(), None))
    return col

def infer_and_convert_data_types(df):
    # change these limits is needed
    null_threshold = len(df) * 0.25
    unique_category_upper_limit = int(len(df)/2)
    unique_category_lower_limit = 100

    for col in df.columns:

        # Check for boolean column
        col_converted = infer_boolean_column(df[col])
        if col_converted.isna().sum() < null_threshold:
            # check column converted to bool.
            df[col] = col_converted

        # Check if the column for categorical
        elif (df[col].dtype == 'category') or (df[col].nunique() <= min(unique_category_lower_limit, unique_category_upper_limit)):
            df[col] = pd.Categorical(df[col])
            df[col] = df[col].astype('category')
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float', errors='coerce')
        elif df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
        elif df[col].dtype == 'datetime':
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:

            # check for numbers
            col_converted = infer_numbers(df[col])
            if col_converted.isna().sum() < null_threshold:
                df[col] = col_converted
                continue

            # check for datetime
            col_converted = infer_date(df[col])
            if col_converted.isna().sum() < null_threshold:
                df[col] = col_converted
                continue

            # check for complex numbers
            col_converted = infer_complex_nums(df[col])
            if col_converted.isna().sum() < null_threshold:
                df[col] = col_converted
                continue

            # check for time delta
            col_converted = infer_timedelta(df[col])
            if col_converted.isna().sum() < null_threshold:
                df[col] = col_converted
                continue

            # else string

    return df

def main(file):
    try:
        # Read file into DataFrame
        df = read_file(file)
        df_len = len(df.index)
        no_of_threads = 1
        if df_len > 10000:
            no_of_threads = max(8, int(df_len / 10000))

        # Use parallel processing to optimize for large datasets
        with concurrent.futures.ProcessPoolExecutor() as executor:
            chunk_dtypes_list = [chunk for chunk in executor.map(process_chunk, np.array_split(df,
                                                                                    no_of_threads))]

        # Aggregate data types across all chunks
        column_types = {}
        for chunk_dtypes in chunk_dtypes_list:
            for col, dtype in chunk_dtypes.items():
                if col not in column_types:
                    column_types[col] = dtype
                else:
                    # If the dtype is category, keep it as category
                    if dtype == 'category':
                        continue
                    # If the column already exists, choose the 'wider' dtype
                    column_types[col] = np.promote_types(column_types[col], dtype)

            return column_types
    except Exception as e:
        return e