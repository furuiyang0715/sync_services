# 传入某只股票以及开始和结束时间
# 生成这段时间内的停牌日
import pprint
import sys
import pymysql
import datetime
import logging

logger = logging.getLogger("main.make_code_sus")


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


def gen_dates(b_date, days):
    day = datetime.timedelta(days=1)
    for i in range(days):
        yield b_date + day*i


def get_date_list(start=None, end=None):
    # 生成一个从 start 到 end 的时间列表，包含 start 和 end
    data = list()
    for d in gen_dates(start, (end-start).days+1):
        data.append(d)
    return data


def gen_inc_code_sus(code, start, end, timestamp):
    # 获取针对 start-end 之间的某只股票的停牌日 包含 start 和 end

    """
    梳理停牌时间与所给时间段的🍌逻辑：

    notice_strat 和 notice_end 是 part 时间段

    要想和一段较长的时间 start 和 end 有交集

    必须满足：

    notice_end> start and notice_start < end

    其中 notice_end 可能为 NULL， 说明停牌到今天，此时就将停牌日设置为 end
    """
    if start > end:
        return

    suspended = list()

    conn = generate_mysqlconnection()

    query_sql = f"""
    select id, NoticeStartDate, NoticeEndDate from stk_specialnotice_new
    where SecuCode = {code} and NoticeTypei = 18 and NoticeTypeii != 1703 
    and NoticeStartDate <= '{end}'
    and UPDATETIMEJZ <= '{timestamp}'
--     and NoticeEndDate >= '{start}'
    ;"""

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
                    # print("no notice start date, wrong.")
                    logger.info("no notice start date, wrong.")
                    continue

                if not notice_end:
                    notice_end = end

                if notice_end < notice_start:
                    logger.info(f"notice_start > notice_end, something has been wrong. code is {code}")
                    sys.exit(1)

                # check 🍌 情况：
                if notice_start == notice_end:
                    # 四点为同一天
                    suspended.append(notice_start)

                elif notice_end <= end:
                    # 停牌的起止完全包含于所给时间的情况
                    suspended.extend(get_date_list(notice_start, notice_end))

                elif notice_start <= start:
                    # 停牌日从 start 到 notice_end
                    suspended.extend(get_date_list(start, notice_end))

                elif notice_end >= end:
                    # 停牌日从 notice_start 到 end
                    suspended.extend(get_date_list(notice_start, end))

                else:
                    # 包含 NoticeEndDate >= '{start}'
                    logger.info(f'未知情况， 未做处理. {code}, {notice_start}, {notice_end}')
                    sys.exit(1)
    finally:
        conn.commit()

    # suspended = sorted(list(set(suspended)))

    return suspended


def make_sus_days(code, start, end, timestamp):

    sus = gen_inc_code_sus(code, start, end, timestamp)

    sus = sorted(list(set(sus)))

    return sus


if __name__ == "__main__":
    codes = ["000031"]
    start = datetime.datetime(2010, 2, 3)
    end = datetime.datetime(2019, 2, 3)
    for code in codes:
        sus = gen_inc_code_sus(code, start, end)
        format_sus = [yyyymmdd_date(s) for s in sus]
        print(pprint.pformat(sus))
