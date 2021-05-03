from datetime import datetime


def convert_to_posix_time(timestamp):
    """Convert a datetime timestamp to posix time (i.e. seconds since epoch: 1st January 1970).

    :param datetime.datetime timestamp:
    :return float:
    """
    return (timestamp - datetime(1970, 1, 1, tzinfo=timestamp.tzinfo)).total_seconds()
