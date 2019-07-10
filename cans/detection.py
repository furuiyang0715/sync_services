import datetime
import logging

import utils
from cans.gen_delisted_days import gen_delisted_days, gen_delisted_info
from cans.gen_market_days import gen_sh000001
from cans.gen_sus_days import gen_inc_code_sus

logger = logging.getLogger()
MARKET_LIMIT_DATE = datetime.datetime(2020, 1, 1)
error_code = list()


def mydetection(ts1, ts2):
    DATACENTER = utils.DC2()
    DATACENTER.execute("use datacenter;")
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
    all_codes = utils.gen_sync_codes()
    # changed 和 all_codes 相交的部分才有意义
    changed = set(all_codes) & changed

    if changed:
        logger.info(f"有改动的数据是 {changed}")
        check_and_update(changed, ts2)
    else:
        logger.info(f"{ts1} 和 {ts2} 之间无更新")


def check_and_update(codes, timestamp):
    cld = utils.gen_calendars_coll()

    limit_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min) + datetime.timedelta(days=1)
    codes_map = utils.convert_front_map(codes)
    market_start = utils.market_first_day()

    market_sus = gen_sh000001(market_start, limit_date, timestamp)

    for code in codes:
        logger.info("")
        logger.info(f"code: {code}")
        # 前缀模式
        f_code = codes_map.get(code)
        logger.info("市场停牌： ")
        # market_sus = gen_sh000001(market_start, limit_date, timestamp)
        logger.info(f"market_sus_0: {market_sus[0]}")
        logger.info(f"market_sus_-1: {market_sus[-1]}")

        logger.info("个股停牌： ")
        code_sus = gen_inc_code_sus(code, market_start, limit_date, timestamp)
        if code_sus:
            logger.info(f"code_sus_0: {code_sus[0]}")
            logger.info(f"code_sus_-1: {code_sus[-1]}")
        else:
            logger.info(f"{code} no suspended days")

        logger.info("个股退市： ")
        infos = gen_delisted_info(code, timestamp)
        delisted = gen_delisted_days(infos, limit_date)
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
        single_sus = sorted(list(set(all_sus) - set(market_sus)))
        single_sus = [utils.yyyymmdd_date(dt) for dt in single_sus]

        # 生成 mongo 数据进行核对
        cursor = cld.find({"code": f_code, "ok": False}, {"date_int": 1, "_id": 0})
        mongo_sus = [j.get("date_int") for j in cursor]

        if sorted(single_sus) == sorted(mongo_sus):
            logger.info(f"check right!")
        else:
            logger.info("check wrong!")
            logger.warning(f"{sorted(single_sus)}")
            logger.warning(f"{sorted(mongo_sus)}")
            # update
            real_sus_dates = set(single_sus) - set(mongo_sus)
            logger.info(f"real_sus_dates: {real_sus_dates}")
            # FIXME 在没有的情况下进行更新 在有的情况下进行插入
            # 但是如果将detection 的时间设置在 inc_sync 之后， 就不会出现 没有某天的数据 的情况
            for sus in real_sus_dates:
                if list(cld.find({"code": f_code, "date_int": sus})):
                    res = cld.update_one({"code": f_code, "date_int": sus}, {"$set": {"ok": False}})
                else:
                    yyyy = int(str(sus)[:4])
                    mm = int(str(sus)[4:6])
                    dd = int(str(sus)[6:])
                    _date = datetime.datetime(yyyy, mm, dd)
                    cld.insert_one({"code": f_code, "date_int": sus, "date": _date, "ok": False})

            real_trading_dates = set(mongo_sus) - set(single_sus)
            logger.info(f"real_trading_dates: {real_trading_dates}")
            # 将其从 mongo 中删除
            cld.delete_many({"code": f_code, "date_int": {"$in": list(real_trading_dates)}})


def check_market_calendar(ts):
    cld = utils.gen_calendars_coll()
    exist = cld.find({"code": "SH000001"}).count()
    market_limit_date = MARKET_LIMIT_DATE
    logger.info(f"同步的截止时间是 {market_limit_date}")
    start = utils.market_first_day()
    logger.info(f"同步的开始时间是 {start}")
    sus = gen_sh000001(start, market_limit_date, ts)
    sh0001_sus = sorted(list(set(sus)))
    if not exist:
        logger.info("market first sync.")
        utils.bulk_insert(cld, "SH000001", sh0001_sus, start=start, end=market_limit_date)
    else:
        already_dates = cld.find({"code": "SH000001", "ok": False}).distinct("date")
        int_already_dates = set([utils.yyyymmdd_date(date) for date in already_dates])
        int_sh0001_sus = set([utils.yyyymmdd_date(d) for d in sh0001_sus])
        if int_sh0001_sus == int_already_dates:
            logger.info("无需更新 .")
        else:
            logger.info("市场交易日历需要重新插入.")


def task():
    ts2 = datetime.datetime.now()
    # 将每次的检测时间回溯到近两天
    ts1 = ts2 - datetime.timedelta(days=1)
    check_market_calendar(ts2)   # 校验日历的更改
    mydetection(ts1, ts2)  # 校验个股的更改


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    task()
