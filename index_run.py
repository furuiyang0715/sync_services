import datetime
import os
import sys
import configparser
import logging.config

from importlib import util

from apscheduler.schedulers.blocking import BlockingScheduler
from daemon import Daemon
from index_sync.index_sync import IndexSync
from utils import LoggerWriter


class MySyncDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err
        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('index')
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
            sched.add_job(self.dummy_sched, 'interval', days=30, start_date=s2)
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


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("./logging.conf")
    config["handler_timedRotatingFileHandler"]["args"] = str(('./logs/index/index.log', 'midnight', 1, 10))

    logging.config.fileConfig(config)
    logger = logging.getLogger('index')

    pid_file = os.path.join(os.getcwd(), "index.pid")
    log_err = LoggerWriter(logger, logging.ERROR)
    index = MySyncDaemon(pidfile=pid_file, log_err=log_err)

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            index.start()
        elif 'stop' == sys.argv[1]:
            index.stop()
        elif 'restart' == sys.argv[1]:
            index.restart()
        elif 'status' == sys.argv[1]:
            index.status()
        else:
            sys.stderr.write("Unknown command\n")
            sys.exit(2)
        sys.exit(0)
    else:
        sys.stderr.write("usage: %s start|stop|restart\n" % sys.argv[0])
        sys.exit(2)
