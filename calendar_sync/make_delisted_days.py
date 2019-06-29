# 传入某只股票以及开始和结束时间
# 生成这段时间内的退市时间

import datetime
import pprint

import pymysql
import logging

logger = logging.getLogger("main.delisted")


def generate_mysqlconnection():
    return pymysql.connect(
        host="139.159.176.118",
        port=3306,
        user="dcr",
        password='acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4',
        charset='utf8mb4',
        db="datacenter"
    )


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


def gen_delisted_info(code, timestamp):
    """
    生成某只股票的全部退市时间信息 info dict 形式

    有关状态的说明： 1-上市，2-暂停上市，3-恢复上市，4-终止上市，5-摘牌，6-退市整理期

    +-----------+----------+---------------------+---------------------+------------+
    | InnerCode | SecuCode | ListedDate          | ChangeDate          | ChangeType |
    +-----------+----------+---------------------+---------------------+------------+
    |       368 | 000693   | 1997-02-26 00:00:00 | 1997-02-26 00:00:00 |          1 |
    |       368 | 000693   | 1997-02-26 00:00:00 | 2007-05-23 00:00:00 |          2 |
    |       368 | 000693   | 1997-02-26 00:00:00 | 2014-01-10 00:00:00 |          3 |
    |       368 | 000693   | 1997-02-26 00:00:00 | 2018-07-13 00:00:00 |          2 |
    |       368 | 000693   | 1997-02-26 00:00:00 | 2019-05-27 00:00:00 |          6 |
    +-----------+----------+---------------------+---------------------+------------+
    :param code:
    :return:
    """

    conn = generate_mysqlconnection()

    # 注意如果不加引号的报错： Warning: (1292, "Truncated incorrect DOUBLE value: 'X11098'")

    query_sql = """
    SELECT A.InnerCode,A.SecuCode,A.ListedDate,B.ChangeDate,B.ChangeType 
    from stk_liststatus B,const_secumainall A WHERE 
    A.InnerCode=B.InnerCode 
    AND A.SecuCode = "{}" AND A.SecuMarket IN(83,90) AND A.SecuCategory=1 
    AND B.ChangeType IN(1,2,3,4,5,6)
    and A.UPDATETIMEJZ<= "{}"
    and B.UPDATETIMEJZ<= "{}";
    """.format(code, timestamp, timestamp)

    infos = list()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query_sql)
            res = cursor.fetchall()
            for column in res:
                # code = column[1],
                listed_date = column[2]
                change_date = column[3]
                change_type = column[4]

                if change_type in [1, 3]:  # 上市和恢复上市
                    infos.append({
                        "code": code,
                        "listed_date": listed_date,
                        "change_date": change_date,
                        "change_type": change_type,
                        "is_listed": True,
                    })
                else:
                    infos.append({
                        "code": code,
                        "listed_date": listed_date,
                        "change_date": change_date,
                        "change_type": change_type,
                        "is_listed": False,
                    })
    finally:
        conn.commit()
    return infos


def gen_delisted_days(infos, limit_date):
    # 生成某只股票的汇总退市时间列表
    # 将 infos 按照 change_date 进行排序
    if not infos:
        return list()

    infos = sorted(infos, key=lambda item: item.get("change_date"))

    delisted_days = list()

    for j in range(len(infos)-1):
        start = infos[j]
        end = infos[j+1]

        # 分为4种情况 （1） True,  True   数据错误 无暂停即恢复
        #            （2） True,  False  上市时间  [ ）
        #            （3） False, False  退市时间  [ ]
        #            （4） False, True   退市时间  [ )

        if start == end:
            logger.info("从上市至今未经历停牌.")
            return list()

        elif start.get("is_listed") and end.get("is_listed"):
            logger.info("数据错误，未经历退市即又上市")

        elif start.get("is_listed") and not end.get("is_listed"):  # True False
            # 将最后一天加入 退市时间
            # delisted_days.append(end.get("change_date"))   # 已有最后一天到截止时间的判断
            pass

        elif not start.get("is_listed") and not end.get("is_listed"):  # False False
            delisted_days.extend(get_date_list(start.get("change_date"), end.get("change_date")))

        elif not start.get("is_listed") and end.get("is_listed"):  # False True
            # 直到重新上市时间的前一天加入退市时间
            delisted_days.extend(get_date_list(start.get("change_date"), end.get("change_date") -
                                               datetime.timedelta(days=1)))

    # 从最后一个时间到今天的判断
    last_info = infos[-1]
    if last_info.get("is_listed"):
        pass
    else:
        # 将最后一个change_date 到今天的时间加到退市时间里面
        delisted_days.extend(get_date_list(last_info.get("change_date"), limit_date))

    # 去除重复值
    # delisted_days = sorted(list(set(delisted_days)))

    return delisted_days


def make_delisted_days(code, limit_date, timestamp):

    infos = gen_delisted_info(code, timestamp)

    if not infos:

        return "no_records"

    delisted = gen_delisted_days(infos, limit_date)

    delisted = sorted(list(set(delisted)))

    return delisted


if __name__ == "__main__":
    code = "000693"

    limit_date = datetime.datetime(2019, 5, 21)

    infos = gen_delisted_info(code)

    # print(pprint.pformat(infos))
    """
    [{'change_date': datetime.datetime(1997, 2, 26, 0, 0),
      'change_type': 1,
      'code': '000693',
      'is_listed': True,
      'listed_date': datetime.datetime(1997, 2, 26, 0, 0)},
      
     {'change_date': datetime.datetime(2007, 5, 23, 0, 0),
      'change_type': 2,
      'code': '000693',
      'is_listed': False,
      'listed_date': datetime.datetime(1997, 2, 26, 0, 0)},
      
     {'change_date': datetime.datetime(2014, 1, 10, 0, 0),
      'change_type': 3,
      'code': '000693',
      'is_listed': True,
      'listed_date': datetime.datetime(1997, 2, 26, 0, 0)},
      
     {'change_date': datetime.datetime(2018, 7, 13, 0, 0),
      'change_type': 2,
      'code': '000693',
      'is_listed': False,
      'listed_date': datetime.datetime(1997, 2, 26, 0, 0)},
      
     {'change_date': datetime.datetime(2019, 5, 27, 0, 0),
      'change_type': 6,
      'code': '000693',
      'is_listed': False,
      'listed_date': datetime.datetime(1997, 2, 26, 0, 0)}]
    """

    delisted = gen_delisted_days(infos, limit_date)

    format_delisted = [yyyymmdd_date(de) for de in delisted]

    d1 = get_date_list(datetime.datetime(2007, 5, 23, 0, 0), datetime.datetime(2014, 1, 10, 0, 0) - datetime.timedelta(days=1))

    d2 = get_date_list(datetime.datetime(2018, 7, 13, 0, 0), limit_date)

    d = d1 + d2

    format_d = [yyyymmdd_date(j) for j in d]

    print(len((set(format_d) - set(format_delisted))))  # 0

    print(yyyymmdd_date(datetime.datetime(2014, 1, 10, 0, 0)) in format_delisted)  # False

    # print(pprint.pformat(format_delisted))
