# 到某一个 limit_date 校验数据库一致性并且更新不一致数据的脚本
# 快照时间戳为当前运行时间
# 在出现问题时只运行一次


import time
import datetime
import sys

import pymongo
import pymysql

import logging

from make_delisted_days import make_delisted_days
from make_market_days import gen_sh000001
from make_sus_days import make_sus_days
from sconfig import SYNC_CONFIG as myconfig

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG,
                    filename=f'logs/total_check_and_update.log',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s')


MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
db = pymongo.MongoClient(MongoUri)

MARKET_LIMIT_DATE = datetime.datetime(2020, 1, 1)

error_code = list()

coll_name = myconfig.get("MONGO_DBNAME", "stock")
cld = db[coll_name]["calendar"]

def gen_sync_codes():
    from all_codes import all
    # codes = list(set(all) - set(hushen300))
    codes = all
    return codes


def generate_mysqlconnection():
    return pymysql.connect(
        host="139.159.176.118",
        port=3306,
        user="dcr",
        password='acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4',
        charset='utf8mb4',
        db="datacenter"
    )


def yyyymmdd_date(dt: datetime) -> int:
    return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day


def gen_limit_date():
    limit_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min) + datetime.timedelta(days=1)
    return limit_date


def check_index():
    # cld.create_index([('code', pymongo.ASCENDING), ('date_int', pymongo.ASCENDING)])
    logger.info(cld.index_information())


def convert_front_map(codes):

    def convert(code):
        if code[0] == "0" or code[0] == "3":
            return "SZ" + code
        elif code[0] == "6":
            return "SH" + code
        else:
            logger.warning("wrong code: ", code)
            sys.exit(1)

    # front_codes = list(map(lambda x: convert(x), codes))
    # print(front_codes)

    res_map = dict()
    for c in codes:
        res_map.update({c: convert(c)})

    return res_map


def gen_dates(b_date, days):
    day = datetime.timedelta(days=1)
    for i in range(days):
        yield b_date + day*i


def get_date_list(start=None, end=None):
    data = list()
    for d in gen_dates(start, (end-start).days+1):
        data.append(d)
    return data


def market_first_day():
    start = None

    conn = generate_mysqlconnection()

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


def _sync_market_calendar():

    exist = cld.find({"code": "SH000001"}).count()

    market_limit_date = MARKET_LIMIT_DATE
    logger.info(f"同步的截止时间是 {market_limit_date}")

    start = market_first_day()
    logger.info(f"同步的开始时间是 {start}")

    sus = gen_sh000001(start, market_limit_date)

    sh0001_sus = sorted(list(set(sus)))

    if not exist:
        logger.info("market first sync.")

        bulk_insert("SH000001", sh0001_sus, start=start, end=market_limit_date)

    else:

        already_dates = cld.find({"code": "SH000001", "ok": False}).distinct("date")

        int_already_dates = set([yyyymmdd_date(date) for date in already_dates])

        int_sh0001_sus = set([yyyymmdd_date(d) for d in sh0001_sus])

        if int_sh0001_sus == int_already_dates:

            logger.info("无需更新 .")

        else:
            logger.info("市场交易日历重新插入.")

            cld.delete_many({"code": "SH000001"})

            bulk_insert("SH000001", sh0001_sus, start=None, end=market_limit_date)


def sync_market_calendar():
    logger.info("sync market calendar start")

    check_index()

    _sync_market_calendar()


def check():
    # 确定一个快照时间戳
    timestamp = datetime.datetime.now()

    logger.info(f"开始检查数据的一致性，本次检查的快照时间戳是 {timestamp}")

    # 检验的截止时间
    limit_date = datetime.datetime(2019, 6, 4)

    # 拿到所有的codes
    codes = gen_sync_codes()

    # for example {'601155': 'SH601155', ...}
    codes_map = convert_front_map(codes)
    market_start = market_first_day()

    for code in codes:
        # 只是为了在日志中空一格 ...
        logger.info("")
        logger.info(f"code: {code}")

        # 前缀模式
        f_code = codes_map.get(code)

        logger.info("市场停牌： ")
        market_sus = gen_sh000001(market_start, limit_date, timestamp)
        logger.info(f"market_sus_0: {market_sus[0]}")
        logger.info(f"market_sus_-1: {market_sus[-1]}")

        # sys.exit(0)

        logger.info("个股停牌： ")
        code_sus = make_sus_days(code, market_start, limit_date, timestamp)
        if code_sus:
            logger.info(f"code_sus_0: {code_sus[0]}")
            logger.info(f"code_sus_-1: {code_sus[-1]}")
        else:
            logger.info(f"{code} no suspended days")

        # sys.exit(0)

        logger.info("个股退市： ")
        delisted = make_delisted_days(code, limit_date, timestamp)

        if delisted == "no_records":
            error_code.append(code)
            continue

        if delisted:
            logger.info(f"delisted_0: {delisted[0]}")
            logger.info(f"delisted_-1: {delisted[-1]}")
        else:
            logger.info(f"{code} no delisted")

        # sys.exit(0)

        # 在最新的 mysql 数据库中查询出的 all_sus
        all_sus = sorted(list(set(market_sus + code_sus + delisted)))
        logger.info(f"all_sus_0: {all_sus[0]}")
        logger.info(f"all_sus_-1: {all_sus[-1]}")
        all_sus = [yyyymmdd_date(dt) for dt in all_sus]

        # 生成 mongo 数据进行核对
        cursor = cld.find({"code": f_code, "ok": False}, {"date_int": 1, "_id": 0})
        mongo_sus = [j.get("date_int") for j in cursor]

        if all_sus == mongo_sus:
            logger.info("check right!")
        else:
            logger.info("check wrong!")
            # update
            real_sus_dates = set(all_sus) - set(mongo_sus)
            logger.info(f"real_sus_dates: {real_sus_dates}")
            for sus in real_sus_dates:
                res = cld.update_one({"code": f_code, "date_int": sus}, {"$set": {"ok": False}})

            real_trading_dates = set(mongo_sus) - set(all_sus)
            logger.info(f"real_trading_dates: {real_trading_dates}")
            for trading in real_trading_dates:
                res = cld.update_one({"code": f_code, "date_int": trading}, {"$set": {"ok": True}})


if __name__ == "__main__":
    try:
        check()
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)
        logger.info(error_code)
