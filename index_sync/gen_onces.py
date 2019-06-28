import os
import sys
import re
import pymysql
import datetime
import logging.config

from importlib import util
from pymongo import MongoClient
from apscheduler.schedulers.blocking import BlockingScheduler

from daemon import Daemon
from sconfig import SYNC_CONFIG as myconfig


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


class IndexSyncDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err

        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('index_sync')
        except ImportError:
            self.setproctitle = False

        self.logger.info("Running into. ")
        try:
            self.dummy_sched()
        except Exception as e:
            self.logger.error(f"定时执行第一次失败，原因是 {e}")
            raise

        try:
            self.scheduler()
        except Exception as e:
            self.logger.info(f"开启定时任务失败，原因是{e}")
            raise

    def scheduler(self):
        sched = BlockingScheduler()
        try:
            sched.add_job(self.dummy_sched, 'interval', days=30)
            sched.start()
        except Exception as e:
            self.logger.error(f'Cannot start scheduler. Error: {e}')
            sys.exit(1)

    def dummy_sched(self):
        try:
            IndexSync().daily_sync()
        except Exception as e:
            self.logger.warning(f"task fail, {e}", exc_info=True)
            sys.exit(1)

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))

# ==================================================================================================


stock_format = [r'^[SI][ZHX]\d{6}$',
                r'^\d{6}\.[A-Z]{4}$']


def little8code(x):
    # 转换为前缀模式
    assert len(x) == 5
    if x == '.XSHG':
        x = 'SH'

    elif x == '.XSHE':
        x = 'SZ'

    elif x == '.INDX':
        x = 'IX'
    return x


def convert_8code(code):
    if re.match(stock_format[1], code):  # 600001.XSHG
        code = little8code(code[6:]) + code[:6]
    elif re.match(stock_format[0], code):  # SH600001
        pass
    else:
        print("股票格式错误 ~")

    return code


# 有关加前缀的映射
end_code_map = {
                "000001": "000001.XSHG",  # 上证指数
                "399001": "399001.XSHE",  # 深证成指
                "399005": "399005.XSHE",  # 中小板指
                "399006": "399006.XSHE",  # 创业板指
                "399004": "399004.XSHE",  # 深证100R
                "399007": "399007.XSHE",  # 深证300
                "399008": "399008.XSHE",  # 中小300
                "000016": "000016.XSHG",  # 上证50
                "000010": "000010.XSHG",  # 上证180指数
                "000009": "000009.XSHG",  # 上证380
                "000300": "000300.XSHG",  # 沪深300
                "000903": "000903.XSHG",  # 中证100
                "000904": "000904.XSHG",  # 中证200
                "000905": "000905.XSHG",  # 中证500
                "000922": "000922.XSHG",  # 中证红利
                "000969": "000969.XSHG",  # 300非周
                "399372": "399372.XSHE",  # 大盘成长
                "399373": "399373.XSHE",  # 大盘价值
                "399374": "399374.XSHE",  # 中盘成长
                "399375": "399375.XSHE",  # 中盘价值
                "399376": "399376.XSHE",  # 小盘成长
                "399377": "399377.XSHE",  # 小盘价值
                "000015": "000015.XSHG",  # 红利指数
                "000019": "000019.XSHG",  # 治理指数
                "000043": "000043.XSHG",  # 超大盘
                "000044": "000044.XSHG",  # 上证中盘
                "399346": "399346.XSHE",  # 深证成长
                "399324": "399324.XSHE",  # 深证红利
                "399328": "399328.XSHE",  # 深证治理
                "399348": "399348.XSHE",  # 深证价值
                "399370": "399370.XSHE",  # 国证成长
                "399366": "399366.XSHE",  # 国证大宗
                "399320": "399320.XSHE",  # 国证服务
                "399321": "399321.XSHE",  # 国证红利
                "399359": "399359.XSHE",  # 国证基建
                "399371": "399371.XSHE",  # 国证价值
                "399362": "399362.XSHE",  # 国证民营
                "399365": "399365.XSHE",  # 国证农业
                "399361": "399361.XSHE",  # 国证商业
                "399322": "399322.XSHE",  # 国证治理
                "399367": "399367.XSHE",  # 巨潮地产
                "399364": "399364.XSHE",  # 中金消费
                "399319": "399319.XSHE",  # 资源优势
                "399673": "399673.XSHE",  # 创业板50
                "399012": "399012.XSHE",  # 创业300
                "399018": "399018.XSHE",  # 创业创新  SZ
                "399608": "399608.XSHE",  # 科技100
                "399612": "399612.XSHE",  # 中创100
                "399550": "399550.XSHE",  # 央视50
                "399360": "399360.XSHE",  # 新硬件   SZ
                "399363": "399363.XSHE",  # 计算机指
                "399415": "399415.XSHE",  # i100
                "000847": "000847.XSHG",  # 腾讯济安  SH
                "399678": "399678.XSHE",  # 深次新股  SZ
                "399016": "399016.XSHE",  # 深证创新  SZ
}


class IndexSync:
    def __init__(self):

        self.mysql_host = "139.159.176.118"
        self.mysql_username = "dcr"
        self.mysql_password = ''
        self.mysql_DBname = "datacenter"
        self.mysql_port = 3306
        self.mongo_host = myconfig.get('MONGO_HOST', "172.17.0.1")
        self.mongo_port = 27017
        self.mongo_DBname = myconfig.get('MONGO_DBNAME', "JQdata")
        self.mongo_collname = "generate_indexcomponentsweight"

        self.check_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min)

    def generate_mysqlconnection(self):
        return pymysql.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_username,
            password=self.mysql_password,
            charset='utf8mb4',
            db=self.mysql_DBname
        )

    def generate_mongo_collection(self, db_name, col_name):
        client = MongoClient(self.mongo_host, self.mongo_port)
        db = client["{}".format(db_name)]
        col = db["{}".format(col_name)]
        return col

    @property
    def index_list(self):
        """
        当前提供的指数列表
        :return:
        """
        return [
                # "399106",  # 深证综指

                "000001",  # 上证指数  000001.XSHG
                "399001",  # 深证成指  399001.XSHE	深证成指
                "399005",  # 中小板指  399005.XSHE	中小板指
                "399006",  # 创业板指  399006.XSHE	创业板指
                "399004",  # 深证100R 399004.XSHE	深证100R
                "399007",  # 深证300  399007.XSHE	深证300
                "399008",  # 中小300  399008.XSHE	中小300
                "000016",  # 上证50   000016.XSHG	上证50
                "000010",  # 上证180  000010.XSHG	上证180指数
                "000009",  # 上证380  000009.XSHG	上证380

                "000300",  # 沪深300  000300.XSHG	沪深300

                "000903",  # 中证100  000903.XSHG	中证100
                "000904",  # 中证200  000904.XSHG	中证200
                "000905",  # 中证500  000905.XSHG	中证500
                "000922",  # 中证红利  000922.XSHG	中证红利
                "000969",  # 300非周  000969.XSHG	300非周
                "399372",  # 大盘成长  399372.XSHE	大盘成长
                "399373",  # 大盘价值  399373.XSHE	大盘价值
                "399374",  # 中盘成长  399374.XSHE	中盘成长
                "399375",  # 中盘价值  399375.XSHE	中盘价值
                "399376",  # 小盘成长  399376.XSHE	小盘成长
                "399377",  # 小盘价值  399377.XSHE	小盘价值
                "000015",  # 红利指数  000015.XSHG	红利指数
                "000019",  # 治理指数  000019.XSHG	治理指数
                "000043",  # 超大盘    000043.XSHG	超大盘
                "000044",  # 上证中盘  000044.XSHG	上证中盘
                "399346",  # 深证成长  399346.XSHE	深证成长
                "399324",  # 深证红利  399324.XSHE	深证红利
                "399328",  # 深证治理  399328.XSHE	深证治理
                "399348",  # 深证价值  399348.XSHE	深证价值
                "399678",  # 深次新股  SZ
                "399016",  # 深证创新  SZ
                "399370",  # 国证成长  399370.XSHE	国证成长
                "399366",  # 国证大宗  399366.XSHE	国证大宗
                "399320",  # 国证服务  399320.XSHE	国证服务
                "399321",  # 国证红利  399321.XSHE	国证红利
                "399359",  # 国证基建  399359.XSHE	国证基建
                "399371",  # 国证价值  399371.XSHE	国证价值
                "399362",  # 国证民营  399362.XSHE	国证民营
                "399365",  # 国证农业  399365.XSHE	国证农业
                "399361",  # 国证商业  399361.XSHE	国证商业
                "399322",  # 国证治理  399322.XSHE	国证治理
                "399367",  # 巨潮地产  399367.XSHE	巨潮地产
                "399364",  # 中金消费  399364.XSHE	中金消费
                "399319",  # 资源优势  399319.XSHE	资源优势
                "399673",  # 创业板50  399673.XSHE	创业板50
                "399012",  # 创业300  399012.XSHE	创业300
                "399018",  # 创业创新  SZ
                "399608",  # 科技100  399608.XSHE	科技100
                "399612",  # 中创100  399612.XSHE	中创100
                "399550",  # 央视50   399550.XSHE	央视50
                "399360",  # 新硬件   SZ  //
                "399363",  # 计算机指  399363.XSHE	计算机指
                "399415",  # I100    399415.XSHE	i100
                "000847"   # 腾讯济安  SH
                ]

    def generate_index_code(self, connection):
        """
        select SeCucode, InnerCode from const_secumainall where SecuCategory=4 AND
        SecuCode in ["000001", "399001"];
        :return:
        """
        query_sql = """
        select SeCucode, InnerCode from const_secumainall where SecuCategory=4 AND SecuCode in {};
                """.format(tuple(self.index_list))
        index_code_dict = dict()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for column in res:
                    index_code_dict.update({column[1]: column[0]})
        finally:
            connection.commit()
        return index_code_dict

    def generate_secucode_weight(self, connection, indexcode, checkdate):
        """
        select SecuCode, Weight from index_indexcomponentsweight where IndexCode = 1 and
        EndDate = (SELECT max(EndDate) FROM index_indexcomponentsweight where IndexCode = 1);
        因为涉及到更新时间是当前这个指数的最新更新时间  所以查询单个进行 而非批量
        """
        query_sql = """select SecuCode, Weight from index_indexcomponentsweight where IndexCode = {}
        and EndDate = (SELECT max(EndDate) FROM index_indexcomponentsweight where IndexCode = {} and EndDate <= '{}');
                        """.format(indexcode, indexcode, checkdate)
        ret_dict = dict()  # bson.errors.InvalidDocument: key '000059.XSHE' must not contain '.'
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for column in res:
                    code = convert_8code(column[0])
                    ret_dict.update({code: float(column[1])})
        finally:
            connection.commit()
        return ret_dict

    def daily_sync(self):
        mysql_con = self.generate_mysqlconnection()

        # 每天同步一次 只插入一条数据 创建的新表是 "generate_indexcomponentsweigh"
        mongo_collection = self.generate_mongo_collection(self.mongo_DBname, self.mongo_collname)

        inner_code_map = self.generate_index_code(mysql_con)

        logger.info(inner_code_map)
        # InnerCode 与 SeCucode 之间的关系
        # {3477: '399319', 3478: '399320', 3479: '399321', 3873: '399322', 4078: '399324', ...}

        inner_code_list = list(inner_code_map.keys())

        checkdates = [
            datetime.datetime(2018, 10, 1),
            datetime.datetime(2018, 11, 1),
            datetime.datetime(2018, 12, 1),
            datetime.datetime(2019, 1, 1),
            datetime.datetime(2019, 2, 1),
            datetime.datetime(2019, 3, 1),
            datetime.datetime(2019, 4, 1),
            datetime.datetime(2019, 5, 1),
            datetime.datetime(2019, 6, 1),
        ]
        for checkdate in checkdates:
            for inner_code in inner_code_list:

                index_secucode_weight_dict = self.generate_secucode_weight(mysql_con, inner_code, checkdate)

                # 更改插入的格式
                end_index_code = end_code_map.get(inner_code_map.get(inner_code))

                front_index_code = convert_8code(end_index_code)

                # to_insert = dict(date=self.check_date,
                to_insert = dict(date=checkdate,

                                 index=front_index_code,    # 使用前缀的形式

                                 index_info=index_secucode_weight_dict)

                mongo_collection.insert_one(to_insert)
                logger.info(f"""insert success: \n
                date: {checkdate} \n
                index: {front_index_code} \n
                index_info: {index_secucode_weight_dict}""")


if __name__ == "__main__":
    # 同步之前记得设置唯一索引 ！！！

    logging.config.fileConfig('./logging.conf')

    logger = logging.getLogger('index_sync')

    IndexSync().daily_sync()
