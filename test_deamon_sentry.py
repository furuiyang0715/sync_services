import datetime
import os
import sys
import configparser
import logging.config
import time

from importlib import util
from apscheduler.schedulers.blocking import BlockingScheduler
from daemon import Daemon
from raven import Client

from utils import LoggerWriter

sentry = Client("https://330e494ccd22497db605a102491c0423@sentry.io/1501024")


class MySyncDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err
        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('calendar')
        except ImportError:
            self.setproctitle = False

        # 设置定时任务的时候首次不会运行
        # 所以在开启定时任务之前先执行一次
        # try:
        #     self.dummy_sched()
        # except Exception as e:
        #     self.logger.error(f"定时执行第一次失败，原因是 {e}")
        #     raise

        # 不使用定时任务调度 直接悬挂 5s 执行一次
        while True:
            t = datetime.datetime.now()
            self.logger.info(f"{t} 开始发送消息 ")
            self.logger.info(f"{sentry}")
            res = sentry.captureMessage(f"{t} 我是一条发送的消息")
            # self.logger.info(f"{res.par}")
            self.logger.info(f"{res}")
            self.logger.info(f"{datetime.datetime.now()} 发送消息结束 ")
            time.sleep(5)

        # try:
        #     self.scheduler()
        # except Exception as e:
        #     self.logger.info(f"开启定时任务失败，原因是{e}")
        #     raise

    def scheduler(self):
        sched = BlockingScheduler()
        try:
            sched.add_job(self.dummy_sched, 'interval', seconds=5)
            sched.start()
        except Exception as e:
            self.logger.error(f'Cannot start scheduler. Error: {e}')
            sys.exit(1)

    def dummy_sched(self):
        try:
            # from raven import Client
            # sentry = Client("https://330e494ccd22497db605a102491c0423@sentry.io/1501024")
            t = datetime.datetime.now()
            logger.info(f"{t} 开始发送消息 ")
            sentry.captureMessage(f"{t} 我是一条发送的消息")
            logger.info(f"{time.time()} 发送消息结束 ")
        except Exception as e:
            self.logger.warning(f"task fail, {e}", exc_info=True)
            sys.exit(1)

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("./logging.conf")
    config["handler_timedRotatingFileHandler"]["args"] = str(('./detection.log', 'midnight', 1, 10))

    logging.config.fileConfig(config)
    logger = logging.getLogger('detection')
    pid_file = os.path.join(os.getcwd(), "detection.pid")
    log_err = LoggerWriter(logger, logging.ERROR)
    detection = MySyncDaemon(pidfile=pid_file, log_err=log_err)
    # 跳过守护进程 直接悬挂终端程序运行
    # detection.run()

    # 守护进程运行
    detection.start()

    # if len(sys.argv) >= 2:
    #     if 'start' == sys.argv[1]:
    #         detection.start()
    #     elif 'stop' == sys.argv[1]:
    #         detection.stop()
    #     elif 'restart' == sys.argv[1]:
    #         detection.restart()
    #     elif 'status' == sys.argv[1]:
    #         detection.status()
    #     else:
    #         sys.stderr.write("Unknown command\n")
    #         sys.exit(2)
    #     sys.exit(0)
    # else:
    #     sys.stderr.write("usage: %s start|stop|restart\n" % sys.argv[0])
    #     sys.exit(2)
