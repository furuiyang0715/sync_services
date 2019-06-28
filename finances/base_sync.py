# -*- coding: utf-8 -*-
import decimal
import sys
import pymongo
import datetime

import logging

import utils
from sconfig import MYSQL_DB

logger = logging.getLogger()

# BaseFinanceSync().daily_sync()


class BaseFinanceSync:
    def __init__(self):
        self.mysql_DBname = MYSQL_DB
        self.check_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min)

    def generate_mongo_collection(self, col_name):
        db = utils.gen_finance_DB()
        col = db["{}".format(col_name)]
        return col

    def gen_mongo_max_id(self, tables):
        ids = dict()
        for table in tables:

            coll = self.generate_mongo_collection(table)
            try:
                id = coll.find().sort([("id", pymongo.DESCENDING)]).limit(1).next().get("id")
            except Exception:
                id = 0
            if not id:
                id = 0
            ids.update({table: id})
        return ids

    def gen_sql_max_id(self, connection, table_name_list):
        if not isinstance(table_name_list, list):
            table_name_list = [table_name_list]

        query_sql = """select max(id) from {};"""

        _res_dict = dict()

        try:
            with connection.cursor() as cursor:
                for table_name in table_name_list:
                    q_sql = query_sql.format(table_name)
                    cursor.execute(q_sql)
                    res = cursor.fetchall()

                    try:
                        table_length = res[0][0]
                    except:
                        raise

                    _res_dict.update({table_name: table_length})
        except Exception:
            raise
        finally:
            connection.commit()
        return _res_dict

    @staticmethod
    def generate_sql_head_name_list(connection, db_name, table_name):
        query_sql = """
        select COLUMN_NAME, DATA_TYPE, column_comment from information_schema.COLUMNS 
        where table_name="{}" and table_schema="{}";
        """.format(table_name, db_name)

        head_name_list = list()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for i in res:
                    head_name_list.append(i[0])
        except Exception:
            raise
        finally:
            connection.commit()
        return head_name_list

    def generate_sql_table_datas_list(self, connection, table_name, name_list, pos):
        try:
            with connection.cursor() as cursor:
                num = 2000
                start = pos
                while True:
                    end = start + num
                    query_sql = """select * from {} where id > {} and  id <= {};""".format(table_name, start, end)
                    logger.info(query_sql)
                    cursor.execute(query_sql)
                    res = cursor.fetchall()
                    start += num
                    yield_column_list = list()
                    for column in res:
                        column_dict = self.zip_doc_dict(name_list, column)
                        yield_column_list.append(column_dict)
                    yield yield_column_list
        except Exception:
            raise
        finally:
            connection.commit()

    @staticmethod
    def zip_doc_dict(name_list, column_tuple):
        if len(name_list) != len(column_tuple):
            return None

        name_tuple = tuple(name_list)
        column_dict = dict(zip(name_tuple, column_tuple))
        return column_dict

    def check_each_sql_table_data(self, dict_data):
        # 对数据进行一致性处理 主要是处理时间

        for key, value in dict_data.items():
            if isinstance(value, decimal.Decimal):
                if value.as_tuple().exponent == 0:
                    dict_data[key] = int(value)
                else:
                    dict_data[key] = float(value)
            elif key in ["PubDate", "EndDate"]:
                # 在同步的过程中直接将其转换为 datetime.datetime 的格式
                if isinstance(value, str):
                    pass
                pass
        return dict_data

    def write_datas2mongo(self, mongo_collection, sql_table_datas_list):
        counter = 0
        for yield_list in sql_table_datas_list:
            if not yield_list and counter <= 10000:
                counter += 1
                continue
            # 一旦有数据就将 counter 清空
            # 做多连续 1000 000 (1000* 每次fetch 的数量)取数据没有拿到 认为生成器已经耗尽
            if not yield_list:
                break
            counter = 0
            j_list = list()
            for j in yield_list:
                j = self.check_each_sql_table_data(j)
                j_list.append(j)
            mongo_collection.insert_many(j_list)

    def do_process(self, last_pos, cur_pos, table_name_list):
        conn = utils.DC()
        for table_name in table_name_list:
            logger.info(table_name)
            logger.info(cur_pos.get(table_name))
            logger.info(last_pos.get(table_name))

            if cur_pos.get(table_name) == last_pos.get(table_name):
                logger.info("{} 当前数据库无更新".format(table_name))
            elif cur_pos.get(table_name) < last_pos.get(table_name):
                logger.warning("{} 当前数据库可能存在删除操作".format(table_name))
                sys.exit(1)
            else:
                logger.info("开始插入更新数据")
                pos = last_pos.get(table_name)
                logger.info(pos)
                head_name_list = self.generate_sql_head_name_list(conn, self.mysql_DBname, table_name)
                logger.info(head_name_list)
                sql_table_datas_list = self.generate_sql_table_datas_list(conn, table_name, head_name_list, pos)
                logger.info(sql_table_datas_list)
                mongo_collection = self.generate_mongo_collection(table_name)
                self.write_datas2mongo(mongo_collection, sql_table_datas_list)

    def daily_sync(self):
        conn = utils.DC()
        sync_tables = utils.gen_finance_sync_tables()
        cur_ids = self.gen_sql_max_id(conn, sync_tables)
        logger.info(f'CUR SQL MAX ID IS {cur_ids}')
        last_ids = self.gen_mongo_max_id(sync_tables)
        logger.info(f'CUR MONGO MAX ISD IS {last_ids}')
        self.do_process(last_ids, cur_ids, sync_tables)


