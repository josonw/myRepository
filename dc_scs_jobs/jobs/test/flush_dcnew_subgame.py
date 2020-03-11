#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
import requests

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import pg_db


def flush_dcnew_subgame():
    '''
    更新子游戏配置
    :param tablename:
    :return:
    '''

    headers = {"User-Agent":
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"}

    url = "http://localhost:26810/new/business/reload/subgame/"
    print url
    req = requests.get(url = url, headers = headers, timeout=1800)
    return req.json()



def main():
    print flush_dcnew_subgame()


if __name__ == '__main__':
    main()
