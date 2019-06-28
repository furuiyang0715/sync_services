import logging
import sys

import utils
from utils import DC

logger = logging.getLogger()


def gen_inc_code_sus(code, start, end, timestamp):
    """
    start 和 end 之间的某只股票的全部停牌日 包含 strat 和 end
    :param code:
    :param start:
    :param end:
    :param timestamp: 进行本次查询的时间戳
    :return:
    """

    # 分为两种情况：
    # （1）在首次更新的时候，我们会在一段较长的 strat 和 end 之间寻找作为时间段存在的 notice_start 和 notice_end
    #  (2) 在增量更新的时候我们往往会判断一个较短的时间段（start 到 end) 是否处于某个停牌期
    #
    # 在第（1） 种情况下， 要想和一段较长的时间 start 和 end 有交集
    #
    # 必须满足：
    #
    # notice_end> start and notice_start < end
    #
    # 其中 notice_end 可能为 NULL， 说明停牌到今天，此时就将停牌日设置为 end

    if start > end:
        return

    suspended = list()

    conn = DC()

    query_sql = f"""
    select id, NoticeStartDate, NoticeEndDate from stk_specialnotice_new
    where SecuCode = {code} and NoticeTypei = 18 and NoticeTypeii != 1703 
    and NoticeStartDate <= '{end}'
    and UPDATETIMEJZ <= '{timestamp}'
--     and NoticeEndDate >= '{start}'
    ;"""
    # print(query_sql)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query_sql)
            res = cursor.fetchall()

            if not res:
                return list()

            for column in res:
                notice_start = column[1]
                notice_end = column[2]

                # 对于数据存在以及合理性的判断 (1) 无 notice start 是错误数据 （2） 无 notice end 置为 end
                # （3） 开始不能大于 结束
                if not notice_start:
                    logger.info("no notice start date, wrong.")
                    continue

                if not notice_end:
                    notice_end = end

                if notice_end < notice_start:
                    logger.info(f"notice_start > notice_end, something has been wrong. code is {code}")
                    sys.exit(1)

                # check相交的情况：
                if notice_start == notice_end:
                    # 四点为同一天
                    suspended.append(notice_start)

                elif notice_end <= end:
                    # 停牌的起止完全包含于所给时间的情况
                    suspended.extend(utils.get_date_list(notice_start, notice_end))

                elif notice_start <= start:
                    # 停牌日从 start 到 notice_end
                    suspended.extend(utils.get_date_list(start, notice_end))

                elif notice_end >= end:
                    # 停牌日从 notice_start 到 end
                    suspended.extend(utils.get_date_list(notice_start, end))

                else:
                    # 包含 NoticeEndDate >= '{start}'
                    logger.info(f'未知情况， 未做处理. {code}, {notice_start}, {notice_end}')
                    sys.exit(1)
    finally:
        conn.commit()
    suspended = sorted(list(set(suspended)))
    return suspended
