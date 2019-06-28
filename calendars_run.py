import datetime
import os
import sys
import configparser
import logging.config

from importlib import util

from apscheduler.schedulers.blocking import BlockingScheduler

from cans.detection import task
from daemon import Daemon


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


class MySyncDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err
        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('detection')
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
            s2 = datetime.datetime(2019, 6, 28, 17, 40, 0)
            sched.add_job(self.dummy_sched, 'interval', minutes=5, start_date=s2)
            sched.start()
        except Exception as e:
            self.logger.error(f'Cannot start scheduler. Error: {e}')
            sys.exit(1)

    def dummy_sched(self):
        try:
            task()
        except Exception as e:
            self.logger.warning(f"task fail, {e}", exc_info=True)
            sys.exit(1)

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("./logging.conf")
    config["handler_timedRotatingFileHandler"]["args"] = str(('./logs/calendars/detection.log', 'midnight', 1, 10))

    logging.config.fileConfig(config)
    logger = logging.getLogger('detection')

    # detection.pid 是检测程序的pid文件
    pid_file = os.path.join(os.getcwd(), "detection.pid")
    log_err = LoggerWriter(logger, logging.ERROR)
    detection = MySyncDaemon(pidfile=pid_file, log_err=log_err)

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            detection.start()
        elif 'stop' == sys.argv[1]:
            detection.stop()
        elif 'restart' == sys.argv[1]:
            detection.restart()
        elif 'status' == sys.argv[1]:
            detection.status()
        else:
            sys.stderr.write("Unknown command\n")
            sys.exit(2)
        sys.exit(0)
    else:
        sys.stderr.write("usage: %s start|stop|restart\n" % sys.argv[0])
        sys.exit(2)
