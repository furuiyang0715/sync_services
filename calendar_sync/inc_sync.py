# 针对具体 code 的增量更新
import os
import sys
import pymongo
import datetime
import logging
from sqlalchemy import create_engine
from importlib import util
import logging.config
from apscheduler.schedulers.blocking import BlockingScheduler
from daemon import Daemon
from sconfig import SYNC_CONFIG as myconfig

nolists = list()


def get_last_date(code):
    MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
    db = pymongo.MongoClient(MongoUri)
    # cld = db.stock.calendar

    coll_name = myconfig.get("MONGO_DBNAME", "stock")
    cld = db[coll_name]["calendar"]

    f_code = code_convert(code)
    cursor = cld.find({"code": f_code}, {"date": 1}).sort([("date", pymongo.DESCENDING)]).limit(1)
    try:
        date = cursor.next().get("date")
    except:
        date = None
    return date


def gen_dates(b_date, days):
    day = datetime.timedelta(days=1)
    for i in range(days):
        yield b_date + day*i


def get_date_list(start=None, end=None):
    data = list()
    for d in gen_dates(start, (end-start).days+1):
        data.append(d)
    return data


def yyyymmdd_date(dt: datetime) -> int:
    return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day


def code_convert(code):
    if code[0] == "0" or code[0] == "3":
        return "SZ" + code
    elif code[0] == "6":
        return "SH" + code
    else:
        logger.warning(f"wrong code: {code}")
        sys.exit(1)


def inc_insert(code, day, ok):
    MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
    db = pymongo.MongoClient(MongoUri)
    # cld = db.stock.calendar

    coll_name = myconfig.get("MONGO_DBNAME", "stock")
    cld = db[coll_name]["calendar"]

    data = {"code": code_convert(code),
            "date": day,
            "date_int": yyyymmdd_date(day),
            "ok": ok}
    logger.info(data)
    logging.info("")
    logging.info("")
    logging.info("")
    cld.insert_one(data)


def inc_sync(code, limit_date):
    conf = {
        "user": "dcr",
        "password": "acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4",
        "host": "139.159.176.118",
        "port": 3306,
        "sqlDBname": "datacenter"
    }
    mysql_string = f"mysql+pymysql://{conf['user']}:{conf['password']}@{conf['host']}:\
        {conf.get('port')}/{conf['sqlDBname']}?charset=gbk"
    DATACENTER = create_engine(mysql_string)

    last_date = get_last_date(code)
    logger.info(last_date)
    if not last_date:
        logger.info(f"请先进行首次更新{code}")
        nolists.append(code)
    elif last_date == limit_date:
    # elif False:
        logger.info(f'{code} 无需更新')
    else:
        inc_days = get_date_list(last_date, limit_date)
        inc_days.remove(last_date)
        logger.info(f"code: {code}")
        logger.info(f"inc days: {inc_days}")
        for inc_day in inc_days:
            logger.info(f"inc_day: {inc_day}")
            # (1) 判断是否是交易日
            query1 = f"""select IfTradingDay from const_tradingday where Date = '{inc_day}' 
            and CMFTime = (select max(CMFTime) from const_tradingday where Date = '{inc_day}');"""
            if_trading_day = DATACENTER.execute(query1).first()[0]
            # print(if_trading_day)
            logger.info(f"if_trading_day: {if_trading_day}")
            if if_trading_day == 2:
                ok = False
                inc_insert(code, inc_day, ok)
                continue
            # (2) 判断是否是停牌日
            query2 = f"""select NoticeStartDate, NoticeEndDate from stk_specialnotice_new
             where SecuCode = {code} 
             and NoticeTypei = 18 
             and NoticeTypeii != 1703
             and NoticeStartDate <= '{inc_day}'
             order by NoticeStartDate desc
             limit 1
             ;"""
            cursor = DATACENTER.execute(query2).first()
            if not cursor:
                ok = True
                inc_insert(code, inc_day, ok)
                continue
            notice_end_date = DATACENTER.execute(query2).first()[1]
            logging.info(f"notice_end_date: {notice_end_date}")
            if not notice_end_date:
                if_suspended_day = True
            elif notice_end_date < inc_day:
                if_suspended_day = False
            else:
                if_suspended_day = True
            if if_suspended_day:
                ok = False
                inc_insert(code, inc_day, ok)
                continue
            # (3) 判断是否退市日
            query3 = f"""SELECT 
            A.InnerCode,A.SecuCode, 
--             A.ListedDate,
            B.ChangeDate,B.ChangeType 
            from stk_liststatus B,const_secumainall A 
            WHERE A.InnerCode=B.InnerCode 
            AND A.SecuMarket IN (83,90) 
            AND A.SecuCategory=1 
            AND B.ChangeType IN (1,2,3,4,5,6) 
            AND A.SecuCode = '{code}'
            AND B.ChangeDate <= '{inc_day}'
            order by B.ChangeDate DESC 
            limit 1
            ; """
            cursor = DATACENTER.execute(query3).first()
            if not cursor:
                inc_insert(code, inc_day, True)
                continue
            # 判断是否是退市日
            # 1-上市，2-暂停上市，3-恢复上市，4-终止上市，5-摘牌，6-退市整理期
            type = DATACENTER.execute(query3).first()[3]
            # print(type)
            logger.info(f'type: {type}')
            # sys.exit(0)
            if type in (1, 3):
                if_listed_day = False
            else:
                if_listed_day = True
            if if_listed_day:
                ok = False
                inc_insert(code, inc_day, ok)
                continue
            inc_insert(code, inc_day, True)


def gen_sync_codes():
    from all_codes import all
    codes = all
    return codes


def gen_limit_date():
    limit_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min) + datetime.timedelta(days=1)
    return limit_date


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
            setproctitle.setproctitle('inc_sync')
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
            # 为了在校detection之前已经有下一天的数据 将inc的时间设定为当日的凌晨 2 点
            s = datetime.datetime(2019, 6, 5, 2, 0, 0)
            sched.add_job(self.dummy_sched, 'interval', days=1, start_date=s)
            sched.start()
        except Exception as e:
            self.logger.error(f'Cannot start scheduler. Error: {e}')
            sys.exit(1)

    def dummy_sched(self):
        try:
            limit_date = gen_limit_date()
            codes = gen_sync_codes()
            for code in codes:
                logger.info(code)
                inc_sync(code, limit_date)
            logger.info(nolists)
        except Exception as e:
            self.logger.warning(f"task fail, {e}", exc_info=True)
            sys.exit(1)

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


if __name__ == "__main__":
    logging.config.fileConfig('./logging2.conf')
    logger = logging.getLogger('inc_sync')
    pid_file = os.path.join(os.getcwd(), "inc.pid")
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
