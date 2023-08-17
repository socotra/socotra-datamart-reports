import datetime
import pytz


def timezone_code_from_name(timezone):
    todaynow = datetime.datetime.now(tz=pytz.timezone(timezone))
    return todaynow.strftime("%Z")
