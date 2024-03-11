def convert_dtype_to_readable(dtype):
    """
    Convert Pandas data type to readable text representation.

    Parameters:
    dtype (str): Pandas data type.

    Returns:
    str: Readable text representation of the data type.
    """
    if 'object' in dtype:
        return 'Text'
    elif 'datetime' in dtype:
        return 'Date'
    elif 'float' in dtype or 'int' in dtype:
        return 'Number'
    elif 'category' in dtype:
        return 'Category'
    elif 'timedelta' in dtype:
        return 'Time Difference'
    elif 'bool' in dtype:
        return 'Boolean'
    elif 'complex' in dtype:
        return 'Complex Number'
    else:
        return 'Object'
