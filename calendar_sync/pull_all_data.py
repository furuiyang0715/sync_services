# 在 drop 掉数据库的时候重新拉取的脚本 一般只在初始化时运行一次

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
                    filename='logs/pull_all_data.log',
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
    # 索引在 mongo shell 中建立 这里仅仅查看
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


def bulk_insert(code, suspended, start, end):
    # 保险起见 将 suspend的 去重排序
    suspended = sorted(list(set(suspended)))

    bulk = list()

    dt = start

    if not suspended:
        logger.info("无 sus_bulk")

        for _date in get_date_list(dt, end):
            bulk.append({"code": code, "date": _date, 'date_int': yyyymmdd_date(_date), "ok": True})

    else:
        for d in suspended:  # 对于其中的每一个停牌日
            # 转换为 整点 模式,  为了 {datetime.datetime(1991, 4, 14, 1, 0)} 这样的数据的存在
            d = datetime.datetime.combine(d.date(), datetime.time.min)

            while dt <= d:

                if dt < d:
                    bulk.append({"code": code, "date": dt, "date_int": yyyymmdd_date(dt), "ok": True})
                    # print(f"{yyyymmdd_date(dt)}: True")

                else:  # 相等即为非交易日
                    bulk.append({"code": code, "date": dt, "date_int": yyyymmdd_date(dt), "ok": False})
                    # print(f"{yyyymmdd_date(dt)}: False")

                dt += datetime.timedelta(days=1)

        # print(dt)  # dt 此时已经是最后一个停牌日期 + 1 的状态了
        # print(end)

        # dt > d:  已经跳出停牌日 在(停牌日+1) 到 截止时间 之间均为交易日
        if dt <= end:
            for _date in get_date_list(suspended[-1] + datetime.timedelta(days=1), end):
                bulk.append({"code": code, "date": _date, 'date_int': yyyymmdd_date(_date), "ok": True})

    logger.info(f"{code}  \n{bulk[0]}  \n{bulk[-1]}")

    try:

        cld.insert_many(bulk)

    except Exception as e:

        logger.info(f"批量插入失败 {code}, 原因是 {e}")

        error_code.append(code)


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


def sync():
    logger.info("jq calendar sync start")
    """创建和查看索引"""
    check_index()
    limit_date = gen_limit_date()
    ts = datetime.datetime.now()
    logger.info(f"同步的截止时间是 {limit_date},时间戳是 {ts}")
    codes = gen_sync_codes()
    codes_map = convert_front_map(codes)
    market_start = market_first_day()

    for code in codes:
        logger.info(f"code: {code}")
        # 前缀模式
        f_code = codes_map.get(code)
        logger.info("市场停牌： ")
        market_sus = gen_sh000001(market_start, limit_date, ts)
        logger.info(f"market_sus_0: {market_sus[0]}")
        logger.info(f"market_sus_-1: {market_sus[-1]}")

        logger.info("个股停牌： ")
        code_sus = make_sus_days(code, market_start, limit_date, ts)
        if code_sus:
            logger.info(f"code_sus_0: {code_sus[0]}")
            logger.info(f"code_sus_-1: {code_sus[-1]}")
        else:
            logger.info(f"{code} no suspended days")

        logger.info("个股退市： ")
        delisted = make_delisted_days(code, limit_date, ts)
        if delisted == "no_records":
            error_code.append(code)
            continue
        if delisted:
            logger.info(f"delisted_0: {delisted[0]}")
            logger.info(f"delisted_-1: {delisted[-1]}")
        else:
            logger.info(f"{code} no delisted")

        all_sus = sorted(list(set(market_sus + code_sus + delisted)))
        logger.info(f"all_sus_0: {all_sus[0]}")
        logger.info(f"all_sus_-1: {all_sus[-1]}")

        bulk_insert(f_code, all_sus, start=market_start, end=limit_date)


if __name__ == "__main__":
    # 只检查更新 SH000001
    sync_market_calendar()
    # 更新指定的 codes
    try:
        sync()
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)
        logger.info(error_code)
