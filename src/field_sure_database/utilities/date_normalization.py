from dateutil import parser

def date_normalization(data: object, data_key: str):
    """
    Normalizes dates

    :param (object) data: The data object which you're iterating through
    :param (string) data_key: The key of the data object which you're trying to normalize
    """
    if data[data_key] != None:
        return parser.parse(str(data[data_key])).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return None