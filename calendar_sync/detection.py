import os
import sys
import datetime
from importlib import util

import pymysql
import pymongo
import logging.config

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine

from make_delisted_days import make_delisted_days
from make_market_days import gen_sh000001
from make_sus_days import make_sus_days
from daemon import Daemon
from sconfig import SYNC_CONFIG as myconfig


MARKET_LIMIT_DATE = datetime.datetime(2020, 1, 1)
error_code = list()


class MySyncDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err
        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('detection')
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
            # s1 = datetime.datetime(2019, 6, 4, 15, 30, 0)
            s2 = datetime.datetime(2019, 6, 4, 8, 0, 0)
            # sched.add_job(self.dummy_sched, 'interval', days=1, start_date=s1)
            sched.add_job(self.dummy_sched, 'interval', days=1, start_date=s2)
            sched.start()
        except Exception as e:
            self.logger.error(f'Cannot start scheduler. Error: {e}')
            sys.exit(1)

    def dummy_sched(self):
        try:
            ts2 = datetime.datetime.now()
            ts1 = ts2 - datetime.timedelta(days=2)
            mydetection(ts1, ts2)
            check_market_calendar(ts2)
        except Exception as e:
            self.logger.warning(f"task fail, {e}", exc_info=True)
            sys.exit(1)

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


def mydetection(ts1, ts2):
    cf = {
        "host": "139.159.176.118",
        "port": 3306,
        "user": "dcr",
        "password": "acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4",
        "sqlDBname": "datacenter",
    }
    mysql_string = f"""mysql+pymysql://{cf['user']}:{cf['password']}@{cf['host']}:{cf.get('port')
    }/{cf['sqlDBname']}?charset=gbk"""
    DATACENTER = create_engine(mysql_string)

    delisted_sql = f"""
        SELECT A.SecuCode from stk_liststatus B,const_secumainall A WHERE 
        A.InnerCode=B.InnerCode 
        AND A.SecuMarket IN(83,90) AND A.SecuCategory=1 
        AND B.ChangeType IN(1,2,3,4,5,6)
        and A.UPDATETIMEJZ > "{ts1}"
        and A.UPDATETIMEJZ <= "{ts2}"
        and B.UPDATETIMEJZ > "{ts1}"
        and B.UPDATETIMEJZ <= "{ts2}"; 
        """
    d_changed = list(DATACENTER.execute(delisted_sql))
    if d_changed:
        d_changed = [j[0] for j in d_changed]
    logger.info(f"d_changed: {d_changed}")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    s_sql = f"""
       select SecuCode from stk_specialnotice_new
       where NoticeTypei = 18 and NoticeTypeii != 1703 
       and UPDATETIMEJZ > '{ts1}'
       and UPDATETIMEJZ <= '{ts2}'
       ;"""
    s_changed = list(DATACENTER.execute(s_sql))
    if s_changed:
        s_changed = [j[0] for j in s_changed]
    logger.info(f"s_changed: {s_changed}")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    changed = set(d_changed + s_changed)
    logger.info(f"changed: {changed}")
    all_codes = gen_sync_codes()
    # changed 和 all_codes 相交的部分才有意义
    changed = set(all_codes) & changed

    if changed:
        check_and_update(changed, ts2)
    else:
        logger.info(f"{ts1} 和 {ts2} 之间无更新")


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


def yyyymmdd_date(dt: datetime) -> int:
    return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day


def generate_mysqlconnection():
    return pymysql.connect(
        host="139.159.176.118",
        port=3306,
        user="dcr",
        password='acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4',
        charset='utf8mb4',
        db="datacenter"
    )


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


def check_and_update(codes, timestamp):
    MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
    db = pymongo.MongoClient(MongoUri)

    # cld = db.stock.calendar
    coll_name = myconfig.get("MONGO_DBNAME", "stock")
    cld = db[coll_name]["calendar"]

    limit_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min) + datetime.timedelta(days=1)
    codes_map = convert_front_map(codes)
    market_start = market_first_day()
    for code in codes:
        logger.info("")
        logger.info(f"code: {code}")
        # 前缀模式
        f_code = codes_map.get(code)
        logger.info("市场停牌： ")
        market_sus = gen_sh000001(market_start, limit_date, timestamp)
        logger.info(f"market_sus_0: {market_sus[0]}")
        logger.info(f"market_sus_-1: {market_sus[-1]}")

        logger.info("个股停牌： ")
        code_sus = make_sus_days(code, market_start, limit_date, timestamp)
        if code_sus:
            logger.info(f"code_sus_0: {code_sus[0]}")
            logger.info(f"code_sus_-1: {code_sus[-1]}")
        else:
            logger.info(f"{code} no suspended days")

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

        # 在最新的 mysql 数据库中查询出的 all_sus
        all_sus = sorted(list(set(market_sus + code_sus + delisted)))
        logger.info(f"all_sus_0: {all_sus[0]}")
        logger.info(f"all_sus_-1: {all_sus[-1]}")
        all_sus = [yyyymmdd_date(dt) for dt in all_sus]

        # 生成 mongo 数据进行核对
        cursor = cld.find({"code": f_code, "ok": False}, {"date_int": 1, "_id": 0})
        mongo_sus = [j.get("date_int") for j in cursor]

        if all_sus == mongo_sus:
            logger.info(f"check right!")
        else:
            logger.info("check wrong!")
            # update
            real_sus_dates = set(all_sus) - set(mongo_sus)
            logger.info(f"real_sus_dates: {real_sus_dates}")
            # FIXME 在没有的情况下进行更新 在有的情况下进行插入
            # 但是如果将detection 的时间设置在 inc_sync 之后， 就不会出现 没有某天的数据 的情况
            for sus in real_sus_dates:
                res = cld.update_one({"code": f_code, "date_int": sus}, {"$set": {"ok": False}})

            real_trading_dates = set(mongo_sus) - set(all_sus)
            logger.info(f"real_trading_dates: {real_trading_dates}")
            for trading in real_trading_dates:
                res = cld.update_one({"code": f_code, "date_int": trading}, {"$set": {"ok": True}})


def gen_sync_codes():
    from all_codes import all
    codes = all
    return codes


def check_market_calendar(ts):
    MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
    db = pymongo.MongoClient(MongoUri)
    # cld = db.stock.calendar

    coll_name = myconfig.get("MONGO_DBNAME", "stock")
    cld = db[coll_name]["calendar"]

    exist = cld.find({"code": "SH000001"}).count()
    market_limit_date = MARKET_LIMIT_DATE
    logger.info(f"同步的截止时间是 {market_limit_date}")
    start = market_first_day()
    logger.info(f"同步的开始时间是 {start}")
    sus = gen_sh000001(start, market_limit_date, ts)
    sh0001_sus = sorted(list(set(sus)))
    if not exist:
        logger.info("market first sync.")
        # bulk_insert("SH000001", sh0001_sus, start=start, end=market_limit_date)
    else:
        already_dates = cld.find({"code": "SH000001", "ok": False}).distinct("date")
        int_already_dates = set([yyyymmdd_date(date) for date in already_dates])
        int_sh0001_sus = set([yyyymmdd_date(d) for d in sh0001_sus])
        if int_sh0001_sus == int_already_dates:
            logger.info("无需更新 .")
        else:
            logger.info("市场交易日历需要重新插入.")


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


if __name__ == "__main__":
    logging.config.fileConfig('./logging.conf')
    logger = logging.getLogger('detection')
    # detection.pid 是检测程序的pid文件
    pid_file = os.path.join(os.getcwd(), "detection.pid")
    log_err = LoggerWriter(logger, logging.ERROR)
    detection = MySyncDaemon(pidfile=pid_file, log_err=log_err)

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            detection.start()
        elif 'stop' == sys.argv[1]:
            detection.stop()
        elif 'restart' == sys.argv[1]:
            detection.restart()
        elif 'status' == sys.argv[1]:
            detection.status()
        else:
            sys.stderr.write("Unknown command\n")
            sys.exit(2)
        sys.exit(0)
    else:
        sys.stderr.write("usage: %s start|stop|restart\n" % sys.argv[0])
        sys.exit(2)
