import datetime
import logging

import utils
from utils import DC

logger = logging.getLogger()


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
    :param timestamp: 查询的时间戳
    :return:
    """

    conn = DC()

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
    if not infos:
        return list()
    # 生成某只股票的汇总退市时间列表
    # 将 infos 按照 change_date 进行排序

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
            delisted_days.extend(utils.get_date_list(start.get("change_date"), end.get("change_date")))

        elif not start.get("is_listed") and end.get("is_listed"):  # False True
            # 直到重新上市时间的前一天加入退市时间
            delisted_days.extend(utils.get_date_list(start.get("change_date"), end.get("change_date") -
                                                     datetime.timedelta(days=1)))

    # 从最后一个时间到今天的判断
    last_info = infos[-1]
    if last_info.get("is_listed"):
        pass
    else:
        # 将最后一个change_date 到今天的时间加到退市时间里面
        delisted_days.extend(utils.get_date_list(last_info.get("change_date"), limit_date))

    # 去除重复值
    # delisted_days = sorted(list(set(delisted_days)))

    return delisted_days
