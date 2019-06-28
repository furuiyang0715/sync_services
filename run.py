#!/usr/bin/env python3
# coding=utf8

import subprocess


def start():
    cmd = """
    python index_run.py start; python finance_run.py start; python calendars_run.py start; 
    """
    subprocess.getoutput(cmd)


def stop():
    cmd = """
        python index_run.py stop; python finance_run.py stop; python calendars_run.py stop; 
        """
    subprocess.getoutput(cmd)


if __name__ == '__main__':
    start()

    # stop()
