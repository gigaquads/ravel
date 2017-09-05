import pytz

from datetime import datetime


def utc_now():
    """
    Return a datetime in UTC timezone.
    """
    return datetime.now(pytz.utc)


def to_timestamp(datetime_obj) -> int:
    """
    From datetime object to integer timestamp (seconds)
    """
    if datetime_obj is None:
        return None

    if isinstance(datetime_obj, datetime):
        if datetime_obj.tzinfo is None:
            raise ValueError('datetime object has no timezone')
    elif isinstance(datetime_obj, date):
        datetime_obj = datetime\
            .strptime(str(datetime_obj), "%Y-%m-%d")\
            .replace(tzinfo=pytz.utc)

    epoch = datetime.fromtimestamp(0, pytz.utc)
    return int((datetime_obj - epoch).total_seconds())
