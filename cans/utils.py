import datetime
import pandas as pd
import pymysql

from pymongo import MongoClient
from sqlalchemy import create_engine
from cans.sconfig import MONGO_URL, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MONGO_DB1, MONGO_COLL


def DB():
    cli = MongoClient(MONGO_URL)
    return cli


def gen_calendars_coll():
    return DB()[MONGO_DB1][MONGO_COLL]


# def DC():
#     mysql_string = f"""mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/
#                        {MYSQL_DB}?charset=gbk"""
#
#     cli = create_engine(mysql_string)
#
#     return cli

def DC():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset='utf8mb4',
        db=MYSQL_DB,
    )


def yyyymmdd_date(dt: datetime.datetime) -> int:
    return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day


def get_date_list(start: datetime.datetime = None, end: datetime.datetime = None) -> list:
    dates = pd.date_range(start=start, end=end, freq='1d')
    dates = [date.to_pydatetime(date) for date in dates]
    return dates


if __name__ == "__main__":
    start_time = datetime.datetime(2019, 5, 1)
    end_time = datetime.datetime(2019, 5, 10)
    ret1 = get_date_list(start_time, end_time)
    # print(ret1)

    # print(yyyymmdd_date(start_time))







    pass