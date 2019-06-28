import datetime
import sys

import pymongo

import pandas as pd
import pymysql

import logging

from pymongo import MongoClient
from sqlalchemy import create_engine

from cans import all_codes
from sconfig import (MONGO_URL, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DB,
                     MONGO_DB2, MONGO_DB1, MONGO_COLL_CALENDARS, MONGO_COLL_INDEX)

import re

logger = logging.getLogger()


stock_format = [r'^[SI][ZHX]\d{6}$',
                r'^\d{6}\.[A-Z]{4}$']


def little8code(x):
    # 转换为前缀模式
    assert len(x) == 5
    if x == '.XSHG':
        x = 'SH'

    elif x == '.XSHE':
        x = 'SZ'

    elif x == '.INDX':
        x = 'IX'
    return x


def convert_8code(code):
    if re.match(stock_format[1], code):  # 600001.XSHG
        code = little8code(code[6:]) + code[:6]
    elif re.match(stock_format[0], code):  # SH600001
        pass
    else:
        print("股票格式错误 ~")

    return code


def end_code_map():
    end_code_map = {
                    "000001": "000001.XSHG",  # 上证指数
                    "399001": "399001.XSHE",  # 深证成指
                    "399005": "399005.XSHE",  # 中小板指
                    "399006": "399006.XSHE",  # 创业板指
                    "399004": "399004.XSHE",  # 深证100R
                    "399007": "399007.XSHE",  # 深证300
                    "399008": "399008.XSHE",  # 中小300
                    "000016": "000016.XSHG",  # 上证50
                    "000010": "000010.XSHG",  # 上证180指数
                    "000009": "000009.XSHG",  # 上证380
                    "000300": "000300.XSHG",  # 沪深300
                    "000903": "000903.XSHG",  # 中证100
                    "000904": "000904.XSHG",  # 中证200
                    "000905": "000905.XSHG",  # 中证500
                    "000922": "000922.XSHG",  # 中证红利
                    "000969": "000969.XSHG",  # 300非周
                    "399372": "399372.XSHE",  # 大盘成长
                    "399373": "399373.XSHE",  # 大盘价值
                    "399374": "399374.XSHE",  # 中盘成长
                    "399375": "399375.XSHE",  # 中盘价值
                    "399376": "399376.XSHE",  # 小盘成长
                    "399377": "399377.XSHE",  # 小盘价值
                    "000015": "000015.XSHG",  # 红利指数
                    "000019": "000019.XSHG",  # 治理指数
                    "000043": "000043.XSHG",  # 超大盘
                    "000044": "000044.XSHG",  # 上证中盘
                    "399346": "399346.XSHE",  # 深证成长
                    "399324": "399324.XSHE",  # 深证红利
                    "399328": "399328.XSHE",  # 深证治理
                    "399348": "399348.XSHE",  # 深证价值
                    "399370": "399370.XSHE",  # 国证成长
                    "399366": "399366.XSHE",  # 国证大宗
                    "399320": "399320.XSHE",  # 国证服务
                    "399321": "399321.XSHE",  # 国证红利
                    "399359": "399359.XSHE",  # 国证基建
                    "399371": "399371.XSHE",  # 国证价值
                    "399362": "399362.XSHE",  # 国证民营
                    "399365": "399365.XSHE",  # 国证农业
                    "399361": "399361.XSHE",  # 国证商业
                    "399322": "399322.XSHE",  # 国证治理
                    "399367": "399367.XSHE",  # 巨潮地产
                    "399364": "399364.XSHE",  # 中金消费
                    "399319": "399319.XSHE",  # 资源优势
                    "399673": "399673.XSHE",  # 创业板50
                    "399012": "399012.XSHE",  # 创业300
                    "399018": "399018.XSHE",  # 创业创新  SZ
                    "399608": "399608.XSHE",  # 科技100
                    "399612": "399612.XSHE",  # 中创100
                    "399550": "399550.XSHE",  # 央视50
                    "399360": "399360.XSHE",  # 新硬件   SZ
                    "399363": "399363.XSHE",  # 计算机指
                    "399415": "399415.XSHE",  # i100
                    "000847": "000847.XSHG",  # 腾讯济安  SH
                    "399678": "399678.XSHE",  # 深次新股  SZ
                    "399016": "399016.XSHE",  # 深证创新  SZ
    }
    return end_code_map


def DB():
    cli = MongoClient(MONGO_URL)
    return cli


def gen_calendars_coll():
    return DB()[MONGO_DB2][MONGO_COLL_CALENDARS]


def gen_index_coll():
    return DB()[MONGO_DB1][MONGO_COLL_INDEX]


def DC2():

    #  mysql_string = f"""mysql+pymysql://{cf['user']}:{cf['password']}@{cf['host']}:{cf.get('port')
    #     }/{cf['sqlDBname']}?charset=gbk"""
    mysql_string = f"""mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/
                       {MYSQL_DB}?charset=gbk"""

    # print(mysql_string)

    cli = create_engine(mysql_string)

    return cli


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


def convert_front_map(codes):
    def convert(code):
        if code[0] == "0" or code[0] == "3":
            return "SZ" + code
        elif code[0] == "6":
            return "SH" + code
        else:
            logger.warning("wrong code: ", code)
            sys.exit(1)
    res_map = dict()
    for c in codes:
        res_map.update({c: convert(c)})
    return res_map


def gen_sync_codes():
    codes = all_codes.all_codes
    return codes


def gen_last_mongo_date(code=None):
    # 取出上次同步数据的最后一天 用于增量更新
    # 不同于 calendar 的逻辑 这时候上一次同步的最后时间已经不是最后一个数据了（因为 calendars 只是插入停牌数据）
    # 取出一个所有数据中最大的时间作为上一次时间 还是保持原样 多查询几次 Emmm This is a problem ...
    # 或者就是找个地方记录下数据 但是我觉得还是尽量不使用记录表比较好
    coll = gen_calendars_coll()
    if code:
        f_code = code_convert(code)
        cursor = coll.find({"code": f_code}, {"date": 1}).sort([("date", pymongo.DESCENDING)]).limit(1)
    else:
        cursor = coll.find({}, {"date": 1}).sort([('date', pymongo.DESCENDING)]).limit(1)
    try:
        date = cursor.next().get("date")
    except:
        date = None
    return date


if __name__ == "__main__":
    # start_time = datetime.datetime(2019, 5, 1)
    # end_time = datetime.datetime(2019, 5, 10)
    # ret1 = get_date_list(start_time, end_time)
    # print(ret1)

    # print(yyyymmdd_date(start_time))

    # ret2 = market_first_day()
    # print(ret2)

    # ret3 = gen_limit_date()
    # print(ret3)

    # code = "002911"
    # ret4 = gen_last_mongo_date(code)
    # print(ret4)

    DC2()

    pass







    pass