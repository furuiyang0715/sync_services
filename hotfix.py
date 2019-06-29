import datetime
import time
import logging

import utils
from cans.gen_delisted_days import gen_delisted_info, gen_delisted_days
from cans.gen_market_days import gen_sh000001
from cans.gen_sus_days import gen_inc_code_sus

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
    # for code in all_codes:
    for code in [
        'SZ00002', 'SZ00001', 'SZ00005',
        'SH600001', 'SH600002', 'SH600003', 'SH600005', 'SH600065', 'SH600074', 'SH600092', 'SH600095', 'SH600102', 'SH600141', 'SH600145', 'SH600158', 'SH600181', 'SH600205', 'SH600253', 'SH600263', 'SH600270', 'SH600286', 'SH600296', 'SH600357', 'SH600401', 'SH600432', 'SH600472', 'SH600485', 'SH600510', 'SH600553', 'SH600591', 'SH600607', 'SH600610', 'SH600625', 'SH600627', 'SH600631', 'SH600632', 'SH600646', 'SH600656', 'SH600659', 'SH600669', 'SH600670', 'SH600672', 'SH600680', 'SH600700', 'SH600709', 'SH600752', 'SH600762', 'SH600772', 'SH600786', 'SH600788', 'SH600799', 'SH600806', 'SH600813', 'SH600816', 'SH600832', 'SH600840', 'SH600842', 'SH600852', 'SH600878', 'SH600880', 'SH600899', 'SH600991', 'SH601268', 'SH601299', 'SH603019', 'SH603315', 'SH603386', 'SZ000003', 'SZ000013', 'SZ000015', 'SZ000024', 'SZ000029', 'SZ000033', 'SZ000047', 'SZ000405', 'SZ000406', 'SZ000412', 'SZ000418', 'SZ000508', 'SZ000511', 'SZ000515', 'SZ000522', 'SZ000527', 'SZ000535', 'SZ000542', 'SZ000549', 'SZ000556', 'SZ000562', 'SZ000569', 'SZ000578', 'SZ000583', 'SZ000588', 'SZ000594', 'SZ000602', 'SZ000618', 'SZ000621', 'SZ000653', 'SZ000658', 'SZ000660', 'SZ000675', 'SZ000689', 'SZ000693', 'SZ000699', 'SZ000715', 'SZ000730', 'SZ000748', 'SZ000763', 'SZ000765', 'SZ000769', 'SZ000787', 'SZ000805', 'SZ000817', 'SZ000827', 'SZ000832', 'SZ000866', 'SZ000916', 'SZ000939', 'SZ000956', 'SZ000979', 'SZ000995', 'SZ002070', 'SZ002193', 'SZ002260', 'SZ002263', 'SZ002323', 'SZ002604', 'SZ002645', 'SZ002680', 'SZ002731', 'SZ300028', 'SZ300104', 'SZ300186', 'SZ300192', 'SZ300216', 'SZ300294', 'SZ300372'
                 ]:
        code = code[2:]
        start = None
        # start = utils.gen_last_mongo_date(code)
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

        if add_sus:
            logger.info("="*200)
            logger.info(f"需要新插入的数据: {add_sus} \n 插入数量是： {len(add_sus)}")
            bulk_insert(code, add_sus)
        if del_sus:
            logger.info("*"*200)
            logger.info(f"需要新删除的数据: {del_sus} \n 插入数量是： {len(del_sus)}")
            bulk_delete(code, del_sus)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    inc()
