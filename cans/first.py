# 检查整个市场的非交易日是否有变化

# 个股增量逻辑：
# for code in all: 遍历每一个股票
# 检出其自己的非交易日和最新的市场的非交易日的差别
# 将其与mongo数据库中已经存在的数据进行对比 进行相应的增删改查

# 个股检测更新逻辑
# 同 detection 的逻辑 但是最终只插入个股自己的停牌日

## 对于一个同步来说，分为三种情况,
## 一是首次同步
    # 对于首次同步来说，开始时间是数据库中的最小时间; 结束时间是截止时间；时间戳是程序运行时当下时间。
## 二是增量同步
    # 对于增量同步，开始时间是数据库中上一次记录的时间，结束时间是截止时间，时间戳是程序运行的当下时间
## 三是更改检测
    # 对于更新检测来说，找出一段时间内的变动情况，进行相应的修改处理

import datetime
import sys
import time
import logging


from cans import utils
from cans.all_codes import all_codes
from cans.gen_delisted_days import gen_delisted_info, gen_delisted_days
from cans.gen_market_days import gen_sh000001
from cans.gen_sus_days import gen_inc_code_sus
from cans.utils import DB

logger = logging.getLogger()


def log_method_time_usage(func):
    def wrapped(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        dt = time.time()-start
        if dt > 0.1:
            logger.info(f"[TimeUsage] {func.__module__}.{func.__name__} usage: {dt}")
        return result
    return wrapped


def gen_diff(market_start, end, timestamp):
    market_sus = gen_sh000001(market_start, end, timestamp)
    for code in all_codes:
        start = utils.gen_last_mongo_date(code)
        if not start:
            start = market_start
        sus = gen_inc_code_sus(code, start, end, timestamp)
        delisted_infos = gen_delisted_info(code, timestamp)
        delisted = gen_delisted_days(delisted_infos, end)
        if not sus:
            sus = []
        single_sus = set(market_sus + sus + delisted) - set(market_sus)

        yield code, single_sus


def bulk_insert(code, sus):
    logger.info(f"{code} 进入增加流程")
    coll = utils.gen_calendars_coll()
    bulks = list()
    # 将 code 转换为 带前缀的格式
    f_code = utils.code_convert(code)
    for s in sus:
        bulks.append({"code": f_code, "date": s, "date_int": utils.yyyymmdd_date(s), "ok": False})
    try:
        ret = coll.insert_many(bulks)
    except Exception as e:
        # 批量插入出错的话
        logger.warning(f"批量插入有误，错误的原因是 {e}")


def bulk_delete(code, sus):
    logger.info(f"{code} 进入删除流程")
    coll = utils.gen_calendars_coll()
    f_code = utils.code_convert(code)
    try:
        ret = coll.delete_many({"code": f_code, "date": {"$in": list(sus)}})
    except Exception as e:
        logger.info(f'批量删除有误，错误的原因是 {e}')


def check_mongo_diff(code, single_sus, ALREADYCHECK=False, DEL=False):
    """
    DEL 是个标识位 表示是否根据 singe_sus 来调整删除原有数据 默认是只增加 不删除
    ALREADYCHECK 也是个标志位 表示在插入新数据时，是否对数据库已经有该数据进行检查 默认是增量更新
    数据都是原来没有插入过的 不检查
    :param code:
    :param single_sus:
    :param ALREADYCHECK:
    :param DEL:
    :return:
    """
    logger.info(f"股票代码是：{code} ")
    already_sus = list()

    if ALREADYCHECK:  # 需要对已经有的数据进行插入重复检查
        coll = utils.gen_calendars_coll()
        f_code = utils.code_convert(code)
        cursor = coll.find({"code": f_code, "ok": False}, {"date": 1, "_id": 0})
        already_sus = [r.get("date") for r in cursor]
        add_sus = set(single_sus) - set(already_sus)  # 需要插入的
        # logger.info(f"需要新插入的数据: {add_sus} \n 插入数量是： {len(add_sus)}")
    else:
        add_sus = single_sus

    if DEL:  # 需要检测后面可能又被删除数据
        del_sus = set(already_sus) - set(single_sus)  # 需要删除的
    else:
        del_sus = set()
    return add_sus, del_sus


@log_method_time_usage
def inc():
    end_time = utils.gen_limit_date()
    timestamp = datetime.datetime.now()
    market_start = utils.market_first_day()

    for code, single_sus in gen_diff(market_start, end_time, timestamp):
        add_sus, del_sus = check_mongo_diff(code, single_sus, ALREADYCHECK=True, DEL=True)

        # 优化 累计到一定量再插入
        if add_sus:
            logger.info("="*200)
            logger.info(f"需要新插入的数据: {add_sus} \n 插入数量是： {len(add_sus)}")
            bulk_insert(code, add_sus)
        if del_sus:
            logger.info("*"*200)
            logger.info(f"需要新删除的数据: {del_sus} \n 插入数量是： {len(del_sus)}")
            bulk_delete(code, del_sus)


# if __name__ == "__main__":
#     logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
#     inc()







