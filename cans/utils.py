import datetime
import pymongo

import pandas as pd
import pymysql

from pymongo import MongoClient
from sqlalchemy import create_engine
from cans.sconfig import MONGO_URL, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MONGO_DB1, MONGO_COLL, \
    MONGO_DB2


def DB():
    cli = MongoClient(MONGO_URL)
    return cli


def gen_calendars_coll():
    return DB()[MONGO_DB2][MONGO_COLL]


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


def market_first_day():
    # 生成市场有交易日记录的第一天作为数据起点
    start = None
    conn = DC()
    query_sql = "select Date from const_tradingday where SecuMarket=83 order by Date asc limit 1"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query_sql)
            res = cursor.fetchall()
            for column in res:
                start = column[0]
    finally:
        conn.commit()
    return start


def gen_limit_date():
    # 生成当日的下一天作为同步的截止时间
    limit_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min) + datetime.timedelta(days=1)
    return limit_date


def code_convert(code):
    # 将纯数字转换为带前缀的格式
    if code[0] == "0" or code[0] == "3":
        return "SZ" + code
    elif code[0] == "6":
        return "SH" + code
    else:
        raise ValueError("股票格式有误")


def gen_last_mongo_date(code):
    # 取出上次同步数据的最后一天 用于增量更新
    coll = gen_calendars_coll()
    print(coll.find().next())

    f_code = code_convert(code)
    cursor = coll.find({"code": f_code}, {"date": 1}).sort([("date", pymongo.DESCENDING)]).limit(1)
    try:
        date = cursor.next().get("date")
    except:
        date = None
    return date


if __name__ == "__main__":
    start_time = datetime.datetime(2019, 5, 1)
    end_time = datetime.datetime(2019, 5, 10)
    ret1 = get_date_list(start_time, end_time)
    # print(ret1)

    # print(yyyymmdd_date(start_time))

    # ret2 = market_first_day()
    # print(ret2)

    ret3 = gen_limit_date()
    # print(ret3)

    code = "002911"
    ret4 = gen_last_mongo_date(code)
    print(ret4)







    pass