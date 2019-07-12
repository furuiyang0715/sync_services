# import sentry_sdk
# sentry_sdk.init("https://d63201421a8b498786b44303dc113d5e@sentry.io/1501030")
# with open("hello.py", "r") as f:
#     print(f.readline())

import time
import datetime
from raven import Client

client = Client("https://d63201421a8b498786b44303dc113d5e@sentry.io/1501030")
# # 在需要记录异常的代码上调用 client.captureException() 即可
# try:
#     1 / 0
# except Exception:
#     client.captureException()


# try:
#     1 / 0
# except:
#     client.user_context({
#         'time': time.time(),
#         'data': datetime.datetime.today(),
#     })
#     client.captureException()



# https://330e494ccd22497db605a102491c0423@sentry.io/1501024


# 测试将 logging 集成到 sentry 中
import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

# All of this is already happening by default!
sentry_logging = LoggingIntegration(
    level=logging.INFO,        # Capture info and above as breadcrumbs
    event_level=logging.ERROR  # Send errors as events
)
sentry_sdk.init(
    dsn="https://330e494ccd22497db605a102491c0423@sentry.io/1501024",
    integrations=[sentry_logging]
)

logging.debug("I am ignored")
logging.info("I am a breadcrumb")
logging.error("I am an event", extra=dict(bar=43))
logging.error("An exception happened", exc_info=True)
