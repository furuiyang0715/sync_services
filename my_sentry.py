from raven import Client

myclient = Client("https://330e494ccd22497db605a102491c0423@sentry.io/1501024")


def get_sentry():
    return myclient


