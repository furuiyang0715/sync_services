# 新表：stock.calendars，
# 数据存放逻辑：1）个股只存当天有停牌的，也就是ok为false的，如果是节假日不需要存；2）上证指数正常存放

# （0) 上证的首次导入和更新检测

# （1） 查询出个股停牌

# （2） 减去上证的停牌（上证的停牌仅为节假日） 即为个股单独的停牌日
import os
import pprint
import sys
from importlib import util

import pymongo
import time
import datetime
import pymysql
import logging.config
from apscheduler.schedulers.blocking import BlockingScheduler

from all_codes import all
from daemon import Daemon
from make_market_days import gen_sh000001
from sconfig import SYNC_CONFIG as myconfig


MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
db = pymongo.MongoClient(MongoUri)

coll_name = myconfig.get("MONGO_DBNAME", "stock")
cld = db[coll_name]["calendars"]
old = db[coll_name]['calendar']

# cld = db.stock.calendars   # 只存停牌的表
# old = db.stock.calendar   # 天数齐全的表

# 上证更新的截止时间
MARKET_LIMIT_DATE = datetime.datetime(2020, 1, 1)
# 记录更新过的股票列表
test_codes = list()


def generate_mysqlconnection():
    # 生成 mysql 连接
    return pymysql.connect(
        host="139.159.176.118",
        port=3306,
        user="dcr",
        password='acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4',
        charset='utf8mb4',
        db="datacenter"
    )


def yyyymmdd_date(dt: datetime.datetime) -> int:
    # 将 datetime.datetime 转换为 date_int 的形式
    return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day


def gen_dates(b_date, days):
    day = datetime.timedelta(days=1)
    for i in range(days):
        yield b_date + day*i


def get_date_list(start=None, end=None):
    # 生成起止时间（包含起止时间） 之间的所有的自然日
    data = list()
    for d in gen_dates(start, (end-start).days+1):
        data.append(d)
    return data


def market_first_day():
    # 查询出同步的第一天（起始时间）
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
    # 封装插入的过程： code 是股票代码; sus 是停牌日;  start 是开始时间; end 是结束时间

    # 保险起见 将 suspend的 去重排序
    suspended = sorted(list(set(suspended)))
    bulk = list()
    dt = start
    if not suspended:
        # logger.info("无 sus_bulk")
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
    # logger.info(f"{code}  \n{bulk[0]}  \n{bulk[-1]}")
    try:
        cld.insert_many(bulk)
    except Exception as e:
        # logger.info(f"批量插入失败 {code}, 原因是 {e}")
        # error_code.append(code)
        pass


def check_index():
    logger.info(cld.index_information())
    # print(cld.index_information())   # 查看数据库的索引信息是否设置


def _sync_market_calendar():
    exist = cld.find({"code": "SH000001"}).count()
    market_limit_date = MARKET_LIMIT_DATE
    logger.info(f"上证同步的截止时间是 {market_limit_date}")
    start = market_first_day()
    logger.info(f"上证同步的开始时间是 {start}")
    # 生成 上证 的停牌日
    # ts 是当前的一个更新时间戳
    ts = datetime.datetime.now()
    sus = gen_sh000001(start, market_limit_date, ts)
    sh0001_sus = sorted(list(set(sus)))

    if not exist:   # 不存在的时候进行首次更新
        logger.info("上证交易日历不存在开始进行首次更新 ... ")
        bulk_insert("SH000001", sh0001_sus, start=start, end=market_limit_date)
    else:  # 已存在的时候检测是否有更新
        already_dates = cld.find({"code": "SH000001", "ok": False}).distinct("date")
        int_already_dates = set([yyyymmdd_date(date) for date in already_dates])
        int_sh0001_sus = set([yyyymmdd_date(d) for d in sh0001_sus])
        if int_sh0001_sus == int_already_dates:
            logger.info("上证交易日历无需更新 .")
        else:
            logger.info("市场交易日历更新 需重新插入.")
            cld.delete_many({"code": "SH000001"})
            bulk_insert("SH000001", sh0001_sus, start=None, end=market_limit_date)


def sync_market_calendar():
    logger.info("检测同步上证的交易日历...")
    check_index()
    _sync_market_calendar()


def gen_sync_codes():
    # 生成所有的股票代码
    codes = all
    return codes


def convert_front_map(codes):
    # 生成 code 和 带前缀的 code 之间的映射
    def convert(code):
        if code[0] == "0" or code[0] == "3":
            return "SZ" + code
        elif code[0] == "6":
            return "SH" + code
        else:
            # logger.warning("wrong code: ", code)
            sys.exit(1)
    res_map = dict()
    for c in codes:
        res_map.update({c: convert(c)})
    return res_map


def check_sus():
    # 以下操作均针对 old 查询出个股不同于大盘的非交易日
    # 查询出上证的停牌日
    sh000001_cursor = old.find({"code": "SH000001", "ok": False}, {"date_int": 1, "_id": 0})
    sh000001_sus = set([r.get("date_int") for r in sh000001_cursor])
    codes = gen_sync_codes()
    code_map = convert_front_map(codes)
    for code in codes:
        f_code = code_map.get(code)
        # 检出单只股票的停牌日
        cursor = old.find({"code": f_code, "ok": False}, {"date_int": 1, "_id": 0})
        sus = set([r.get("date_int") for r in cursor])
        # singe_sus 是个股相对于大盘的"单独的"停牌日
        singe_sus = sus - sh000001_sus
        yield (f_code, singe_sus)


def int2dt(date_int):
    # 将 date_int 转换为 datetime.datetime 的格式
    dt_str = str(date_int)
    dt = datetime.datetime(int(dt_str[:4]), int(dt_str[4:6]), int(dt_str[6:]))
    return dt


def false_bulk_insert(f_code, to_add):
    mybulk = list()
    for date_int in to_add:
        mybulk.append({"code": f_code, "date": int2dt(date_int), "date_int": date_int, "ok": False})

    logger.info(mybulk)
    # 批量插入
    try:
        cld.insert_many(mybulk)
    except Exception as e:
        logger.info(f"{f_code} 插入 to_add 失败， 原因是 {e}")
    else:
        logger.info(f"{f_code} 插入成功 ... ")


def bulk_delete(f_code, to_delete):
    # docs = list(cld.find({"code": f_code, "date_int": {"$in": list(to_delete)}}, {"_id": 0, "code": 1, "date_int": 1}))
    # if docs:
    #     print(pprint.pformat(docs))
    try:
        cld.delete_many({"code": f_code, "date_int": {"$in": list(to_delete)}})
    except Exception as e:
        logger.info(f"{f_code} 删除失败， 原因是 {e} ")
    else:
        logger.info(f"{f_code} 删除成功 ... ")


def check_diff(f_code, sus):
    # 检出 cld 和 old 之间存储数据的差异 使用 old 的变化去更新 cld
    cursor = cld.find({"code": f_code, "ok": False}, {"date_int": 1, "_id": 0})
    now = set([r.get("date_int") for r in cursor])
    # print("now: ", now)
    # sus - now 是需要更新进入的数据
    # print("sus - now :", sus - now)
    to_add = sus - now
    if to_add:
        logger.info(f"需要更入的数据有 {to_add}")
        false_bulk_insert(f_code, to_add)

    # now - sus 是需要从 cld 中删除的数据
    # print("now - sus: ", now - sus)
    to_delete = now - sus
    if to_delete:
        logger.info(f"需要删除的数据有 {to_delete}")
        bulk_delete(f_code, to_delete)

    if (sus - now) or (now - sus):
        # test_codes 存的是更新了的股票们 just for look and check
        test_codes.append(f_code)

    if not to_add and not to_delete:
        logger.info(f"{f_code} 无需改动 ...")

    logger.info("")
    logger.info("")
    logger.info("")


def process():
    t1 = time.time()
    # process ...
    # 进行 上证的检测和更新
    sync_market_calendar()
    # 一个每次生成个股单独停牌的生成器
    yie = check_sus()
    # logger.info(yie)
    for f_code, singe_sus in yie:
        logger.info(f"开始对 {f_code} 进行更新")
        # logger.info("last: ", singe_sus)
        check_diff(f_code, singe_sus)

    logger.info(f"进行了改动的股票列表是： {test_codes}")

    t2 = time.time()
    logger.info(f"同步所需的时间是 {(t2 - t1) / 60} min ")


#  - - - - - - - - - - - - - - -- - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - -

class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


class MySyncDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err
        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('single_sync')
        except ImportError:
            self.setproctitle = False
        self.logger.info("Running into. ")
        try:
            self.dummy_sched()
        except Exception as e:
            self.logger.error(f"定时执行第一次失败，原因是 {e}")
            raise
        try:
            self.scheduler()
        except Exception as e:
            self.logger.info(f"开启定时任务失败，原因是{e}")
            raise

    def scheduler(self):
        sched = BlockingScheduler()
        try:
            # s = datetime.datetime(2019, 6, 4, 18, 0, 0)
            # 撸完一次大约 5 min 左右
            # 在正常的交易时间是不会有更新的

            # inc 更新的时间是 每日凌晨 2 点
            # detection 的时间是 8 点
            # 那就定时间为 早上 8 点 40
            s = datetime.datetime(2019, 6, 13, 8, 40, 0)
            sched.add_job(self.dummy_sched, 'interval', days=1, start_date=s)
            sched.start()
        except Exception as e:
            self.logger.error(f'Cannot start scheduler. Error: {e}')
            sys.exit(1)

    def dummy_sched(self):
        try:
            process()
        except Exception as e:
            self.logger.warning(f"task fail, {e}", exc_info=True)
            sys.exit(1)

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


if __name__ == "__main__":
    logging.config.fileConfig('./logging.conf')
    logger = logging.getLogger('single')
    pid_file = os.path.join(os.getcwd(), "single.pid")
    log_err = LoggerWriter(logger, logging.ERROR)
    single = MySyncDaemon(pidfile=pid_file, log_err=log_err)

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            single.start()
        elif 'stop' == sys.argv[1]:
            single.stop()
        elif 'restart' == sys.argv[1]:
            single.restart()
        elif 'status' == sys.argv[1]:
            single.status()
        else:
            sys.stderr.write("Unknown command\n")
            sys.exit(2)
        sys.exit(0)
    else:
        sys.stderr.write("usage: %s start|stop|restart\n" % sys.argv[0])
        sys.exit(2)


# 本地测试
# if __name__ == "__main__":
#     logging.config.fileConfig('./logging.conf')
#     logger = logging.getLogger('single')
#     process()
