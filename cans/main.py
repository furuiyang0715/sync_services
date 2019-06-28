# 检查整个市场的非交易日是否有变化

# 个股增量逻辑：
# for code in all: 遍历每一个股票
# 检出其自己的非交易日和最新的市场的非交易日的差别
# 将其与mongo数据库中已经存在的数据进行对比 进行相应的增删改查

# 个股检测更新逻辑
# 同 detection 的逻辑 但是最终只插入个股自己的停牌日

import datetime
import sys
import time

from cans import utils
from cans.all_codes import all_codes
from cans.gen_delisted_days import gen_delisted_info, gen_delisted_days
from cans.gen_market_days import gen_sh000001
from cans.gen_sus_days import gen_inc_code_sus
from cans.utils import DB


def log_method_time_usage(func):
    def wrapped(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        dt = time.time()-start
        if dt > 0.1:
            print(f"[TimeUsage] {func.__module__}.{func.__name__} usage: {dt}")
        return result
    return wrapped


def gen_diff(start, end, timestamp):
    market_sus = gen_sh000001(start, end, timestamp)
    for code in all_codes:
        sus = gen_inc_code_sus(code, start, end, timestamp)
        # print("sus: ", sus)
        delisted_infos = gen_delisted_info(code, timestamp)
        delisted = gen_delisted_days(delisted_infos, end)
        # print("delisted: ", delisted)
        single_sus = set(market_sus + sus + delisted) - set(market_sus)
        yield code, single_sus


def bulk_insert(code, sus):
    print(f"{code} 进入增加流程")
    coll = utils.gen_calendars_coll()
    bulks = list()
    # 将 code 转换为 带前缀的格式
    f_code = utils.code_convert(code)
    for s in sus:
        bulks.append({"code": f_code, "date": s, "date_int": utils.yyyymmdd_date(s), "ok": False})
    try:
        print(bulks)
        ret = coll.insert_many(bulks)
        print(ret)
    except Exception as e:
        print(e)
        pass


def bulk_delete(code, sus):
    print(f"{code} 进入删除流程")
    coll = utils.gen_calendars_coll()
    try:
        ret = coll.delete_many({"code": code, "date": {"$in": sus}})
        print(ret)
    except Exception as e:
        print(e)
        pass


def check_mongo_diff(code, single_sus):
    coll = utils.gen_calendars_coll()
    cursor = coll.find({"code": code, "ok": False}, {"date": 1, "_id": 0})
    already_sus = [r.get("date") for r in cursor]
    print("数据库已经存在的数据: ", already_sus)
    add_sus = set(single_sus) - set(already_sus)  # 需要插入的
    del_sus = set(already_sus) - set(single_sus)  # 需要删除的
    return add_sus, del_sus


@log_method_time_usage
def main():
    start_time = utils.market_first_day()
    # end_time = utils.gen_limit_date()
    end_time = datetime.datetime(2019, 3, 1)
    ts = datetime.datetime.now()
    for code, single_sus in gen_diff(start_time, end_time, ts):
        print(code, single_sus)
        add_sus, del_sus = check_mongo_diff(code, single_sus)
        print(code, add_sus, "\n",  del_sus)

        # 对 add_sus 进行插入
        if add_sus:
            bulk_insert(code, add_sus)
        # 对 del_sus 进行删除
        if del_sus:
            bulk_delete(code, del_sus)


def inc():
    # 增量
    # start_time =
    pass


def detection():
    pass


if __name__ == "__main__":

    main()



## 对于一个同步来说，分为三种情况,
## 一是首次同步
    # 对于首次同步来说，开始时间是数据库中的最小时间; 结束时间是截止时间；时间戳是程序运行时当下时间。
## 二是增量同步
    # 对于增量同步，开始时间是数据库中上一次记录的时间，结束时间是截止时间，时间戳是程序运行的当下时间
## 三是更改检测
    # 对于更新检测来说，找出一段时间内的变动情况，进行相应的修改处理


