import re
from datetime import datetime, timezone
from stringcase import snakecase


def clean(data, date):
    """Clean up data from meteorological mast anemometers and wind vanes...

    ... or rename the function and do pretty much whatever you want with the data!

    In this ultra-simple example, we'll parse the time and add the date to it (timestamps are much more useful when
    complete!) as well as correcting a typo in column headings

    :parameter data: Pandas dataframe containing imported, uncleaned data
    :type data: pandas.dataframe

    :return: The same dataframe, cleaned.
    :rtype: pandas.dataframe
    """

    # Add a proper datestamp column (combining the time from the time column with the date from the metadata)
    cleaned_timestamps = []
    for ts in data["TimeStamp"]:
        # Decimal minutes. REALLY? This is why our work is so hard. Learn from the gross lack of foresight of whoever
        # made this meteorological mast, and put data into sensible formats (ISO formatted timestamp would've been
        # really helpful here!).
        hms = [int(value) for value in re.split(r"[:.]", ts)]
        hms[2] = hms[2] * 6
        cleaned_timestamps.append(
            datetime(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=hms[0],
                minute=hms[0],
                second=hms[1],
                tzinfo=timezone.utc,  # UTC is a best guess since no timezone info supplied with the data.
            )
        )

    # Replace the near-useless decimal-minutes-no-date-no-timezone with a cleaned, complete datestamp
    data.pop("TimeStamp")
    data["time_stamp"] = cleaned_timestamps  # If storage space is a premium, convert this to posix time

    # And clean up typos made by an overly stressed out junior engineer who is too busy to care about the pain this
    # has caused you finding it
    data["Barometer_1"] = data.pop("Barmoeter_1")

    # And convert column names to snake_case, so they make sensible variable names without spaces etc
    data.rename(columns=lambda old_name: snakecase(old_name))

    return data
