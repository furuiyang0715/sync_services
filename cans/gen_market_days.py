import datetime

import utils
from utils import DC


def gen_sh000001(start, end, timestamp):
    """
    对于整个市场, 从 start 到 end 之间非交易日列表 suspended
    数据源为： JQdata.const_tradingday
    包含 start 和 end
    """
    bulk = list()

    # （1） mongo的形式
    # source = db.JQdata.const_tradingday
    # suspended = source.find(
    #     {"IfTradingDay": 2, "SecuMarket": 83, "Date": {"$gte": start, "$lte": end}}, {"Date": 1}
    # ).sort("Date", pymongo.ASCENDING).distinct("Date")
    # suspended = sorted(suspended)

    # （2） mysql 的形式
    conn = DC()

    suspended = list()
    trading = list()

    # REF:  https://blog.csdn.net/wild46cat/article/details/78715099

    # （1）查询出所有标记为 非交易日的
    # （2）查询出所有标记为 交易日的
    # （3）根据 CMFTime 判断到底哪个是 ✅ 的

    # （1）
    sus_sql = f"""
    select distinct(Date) from const_tradingday where 
    IfTradingDay=2
    and SecuMarket=83
    and Date >="{start}"
    and Date <= "{end}"
    and UPDATETIMEJZ <= "{timestamp}"
    order by Date asc;
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(sus_sql)
            res = cursor.fetchall()
            for column in res:
                suspended.append(column[0])
    finally:
        conn.commit()
    suspended = sorted(list(set(suspended)))

    # （2）
    trade_sql = f"""
    select distinct(Date) from const_tradingday where 
    IfTradingDay=1
    and SecuMarket=83
    and Date >="{start}"
    and Date <= "{end}"
    and UPDATETIMEJZ <= "{timestamp}"
    order by Date asc;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(trade_sql)
            res = cursor.fetchall()
            for column in res:
                trading.append(column[0])
    finally:
        conn.commit()
    trading = sorted(list(set(trading)))

    # （3）
    for day in (set(trading) & set(suspended)):
        check_sql = f"""
        select IfTradingDay from const_tradingday where Date = "{day}" and 
        UPDATETIMEJZ = (select max(UPDATETIMEJZ) from const_tradingday where Date = "{day}");
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(check_sql)
                res = cursor.fetchall()
                iftrading = res[0][0]
        finally:
            conn.commit()

        if iftrading == 1:
            suspended.remove(day)
        if iftrading == 2:
            trading.remove(day)

    # return suspended, trading
    return suspended


if __name__ == "__main__":
    start = datetime.datetime(2005, 1, 4)
    end = datetime.datetime(2019, 12, 31)

    sus, trading = gen_sh000001(start, end)
    trading = [utils.yyyymmdd_date(day) for day in trading]

    print(len(trading))
    print(trading[0])
    print(trading[-1])

    # 使用 JQdata 接口的数据做测试
    import jqdatasdk as jqsdk

    jqsdk.auth('15626046299', '046299')
    # print(type(jqsdk.get_all_trade_days()))

    res1 = jqsdk.get_all_trade_days()
    res2 = res1.tolist()
    res2 = [utils.yyyymmdd_date(day) for day in res2]
    print(len(res2))
    print(res2[0])  # 2005-01-04
    print(res2[-1])  # 2019-12-31
    print(trading == res2)
