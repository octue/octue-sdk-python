from datetime import datetime, timedelta


def convert_to_posix_time(timestamp):
    """Convert a datetime timestamp to posix time (i.e. seconds since epoch: 1st January 1970).

    :param datetime.datetime timestamp:
    :return float:
    """
    return (timestamp - datetime(1970, 1, 1, tzinfo=timestamp.tzinfo)).total_seconds()


def convert_from_posix_time(posix_timestamp):
    """Convert a posix timestamp to a datetime timestamp.

    :param int|float posix_timestamp:
    :return datetime.datetime:
    """
    return datetime(1970, 1, 1) + timedelta(seconds=posix_timestamp)
