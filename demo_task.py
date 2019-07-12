import datetime
import time
import logging
logger = logging.getLogger()


def task(sentry):
    logger.info(f"{sentry}")
    try:
        # sentry.captureMessage(f"实盘 <{env.config.base.task_id}> 开始运行")
        t = datetime.datetime.now()
        logger.info(f"{t} 我开始运行啦")
        sentry.captureMessage(f"{t} 我开始运行啦")
    except Exception as e:
        # sentry.captureException(exc_info=True, extra=env.config.base)
        sentry.captureException(exc_info=True, extra={"add_info": "出错了, 我是额外补充的信息"})
        logger.info(f"{e}")


if __name__ == "__main__":
    from raven import Client
    sentry = Client("https://330e494ccd22497db605a102491c0423@sentry.io/1501024")
    while True:
        task(sentry)
        time.sleep(4)
