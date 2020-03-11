#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from datetime import datetime
sys.path.append('/data/wwwroot/scs_server')

from libs.warning_way import send_sms


def main():
    print(datetime.now())
    print('execute ps -ef | grep "scs_loop" | grep -v grep | wc -l')
    stdout, stderr = os.system('ps -ef | grep "scs_loop" | grep -v grep | wc -l')

    print('execute result is: stdout:%s, stderr:%s' % (stdout, stderr))

    # 如果没有scs_loop进程
    if (stdout, stderr) == (0, 0):
        print('scs_loop is gone, seed message for manager')
        send_sms(['TomZhu'], '调度系统已经挂了，挂了!!!', 8)


if __name__ == '__main__':
    main()
