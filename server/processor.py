import pandas as pd
from dateutil.parser import parse
import numpy as np
import re, io
import concurrent.futures


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
    return chunk


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
    return x


def infer_complex_nums(col):
    col = col.apply(to_complex)
    return col


def infer_timedelta(col):
    col = pd.to_timedelta(col, errors='coerce')
    return col


def convert_to_bool(value):
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        if value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
    return None


def infer_and_convert_data_types(df):
    null_threshold = len(df) * 0.25

    for col in df.columns:

        # Convert the column to boolean
        col_converted = df[col].apply(convert_to_bool)
        if col_converted.isna().sum() < null_threshold:
            # check column converted to bool.
            df[col] = col_converted

        # Check if the column should be categorical
        # elif len(df[col].unique()) / len(df[col]) < 0.5:  # Example threshold for categorization
        elif len(df[col].unique()) < min(100, int(len(df[col]) / 2)):  # Example threshold for categorization
            df[col] = pd.Categorical(df[col])
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float', errors='coerce')
        elif df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
        elif df[col].dtype == 'datetime':
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:
            # check for time delta
            col_converted = infer_timedelta(df[col])
            if col_converted.isna().sum() < null_threshold:
                df[col] = col_converted
                continue

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

            # else string

    return df


def main(file):
    # Read file into DataFrame
    df = read_file(file)
    df_len = len(df.index)
    no_of_threads = 1
    if df_len > 10000:
        no_of_threads = int(df_len / 10000)

    # Use parallel processing to optimize for large datasets
    with concurrent.futures.ProcessPoolExecutor() as executor:
        chunks = [chunk for chunk in executor.map(process_chunk, np.array_split(df,
                                                                                no_of_threads))]  # Adjust the number of splits based on available cores

    # Concatenate processed chunks
    df = pd.concat(chunks, ignore_index=True)

    # Display DataFrame info with data types
    # df_info = df.info()
    column_types = {}
    for column in df.columns:
        column_types[column] = str(df[column].dtype)

    # Convert the dictionary to JSON format
    # json_format = json.dumps(column_types)

    return column_types
