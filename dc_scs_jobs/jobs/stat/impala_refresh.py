# coding: utf-8

""" @author: LukeHu
    @func: impala中执行 refresh 和 invalidate metadata ，以刷新元数据。
"""

import os
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.ImpalaSql import ImpalaSql


def main():
    impala = ImpalaSql(host='192.168.0.96')
    impala.exe_sql("""invalidate metadata;
                      refresh analysis.top_pay_fct;
                      refresh analysis.toppay_user_note;
                      refresh analysis.bpid_platform_game_ver_map;
                      refresh analysis.payuser_gameparty_game_type_fct;
                   """)


if __name__ == '__main__':
    main()
