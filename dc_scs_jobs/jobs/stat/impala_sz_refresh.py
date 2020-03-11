# coding: utf-8

""" @author: tommyjiang
    @func: impala中执行 refresh 和 invalidate metadata ，以刷新元数据。
"""

import os
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.ImpalaSql import ImpalaSql


def main():
    impala = ImpalaSql(host='HKWF-USLC-175-45-5-207.boyaa.com')
    impala.exe_sql("""invalidate metadata;""")


if __name__ == '__main__':
    main()
