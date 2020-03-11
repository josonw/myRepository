#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import re
import time
import datetime
from cStringIO import StringIO
import traceback

import pyhs2
import commands
import cx_Oracle
import signal
import threading
import paramiko

comare_stg = None
f_log      = None


def write_log(write_info):
    global f_log

    log_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    log_info = str(log_time) + ": " + write_info + '\n'

    f_log.write(log_info)
    f_log.flush()


class ShellExec(object):
    host = '192.168.0.94'
    port = 3600
    user = 'hadoop'
    pkey = '/home/hadoop/.ssh/id_rsa'

    def __init__(self):
        self._connect()

    def _connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.load_system_host_keys()
        self.ssh.connect(self.host, self.port, self.user, self.pkey)

    def execute(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdout.read(), stderr.read()


class CompareSTG:
    def __init__(self, oracle_conn, hive_conn, table_list_file, date):
        self.oracle_conn        = oracle_conn
        self.hive_conn          = hive_conn
        self.table_list_file    = table_list_file
        self.date               = date

        self.write_io = StringIO()
        self.err_io   = StringIO()

    def __del__(self):
        self.write_io.close()
        self.err_io.close()


    def query_datas(self, conn, sql):
        cur = conn.cursor()
        datas = []

        t = threading.Timer(3*60, conn.cancel)
        t.start()

        try:
            cur.execute( sql )
            datas = cur.fetchall()
        except Exception, e:
            print e
        finally:
            cur.close()
            t.cancel()

        return datas

    def query_one(self, conn, sql):
        # 建立Cursor对象
        cur = conn.cursor()
        datas = (0,)

        t = threading.Timer(3*60, conn.cancel)
        t.start()

        # 插叙数据，并获取结果
        try:
            cur.execute( sql )
            datas = cur.fetchone()
        except Exception, e:
            self.err_io.write( "err_sql:%s\n" % sql )
            self.err_io.write( "trace:")
            traceback.print_exc(file=self.err_io)
        finally:
            cur.close()
            t.cancel()

        return datas[0]


    def get_hive_data_path(self, table_name):
        table_path_dict = {
            # 原始表
            'by_event_args_stg':            'hadoop fs -cat /dw/stage/by_event_args_stg/%(date)s/%(date)s* | wc -l',
            'user_signup_stg':              'hadoop fs -cat /dw/stage/user_signup/%(date)s/%(date)s* | wc -l',
            'user_login_stg':               'hadoop fs -cat /dw/stage/user_login/%(date)s/%(date)s* | wc -l',
            'user_logout_stg':              'hadoop fs -cat /dw/stage/user_logout/%(date)s/%(date)s* | wc -l',
            'pb_gamecoins_stream_stg':      'hadoop fs -cat /dw/stage/pb_gamecoins_stream/%(date)s/%(date)s* | wc -l',
            'pb_bycoins_stream_stg':        'hadoop fs -cat /dw/stage/pb_bycoins_stream/%(date)s/%(date)s* | wc -l',
            'pb_props_stream_stg':          'hadoop fs -cat /dw/stage/pb_props_stream/%(date)s/%(date)s* | wc -l',
            'pb_gifts_stream_stg':          'hadoop fs -cat /dw/stage/pb_gifts_stream/%(date)s/%(date)s* | wc -l',
            'user_bankrupt_stg':            'hadoop fs -cat /dw/stage/user_bankrupt/%(date)s/%(date)s* | wc -l',
            'user_bankrupt_relieve_stg':    'hadoop fs -cat /dw/stage/user_bankrupt_relieve/%(date)s/%(date)s* | wc -l',
            'day_user_balance_stg':         'hadoop fs -cat /dw/stage/day_user_balance/%(date)s/%(date)s* | wc -l',
            'game_activity_stg':            'hadoop fs -cat /dw/stage/game_activity/%(date)s/%(date)s* | wc -l',
            'user_gameparty_stg':           'hadoop fs -cat /dw/stage/gameparty/%(date)s/%(date)s* | wc -l',
            'finished_gameparty_stg':       'hadoop fs -cat /dw/stage/finished_gameparty/%(date)s/%(date)s* | wc -l',
            'join_gameparty_stg':           'hadoop fs -cat /dw/stage/join_gameparty/%(date)s/%(date)s* | wc -l',
            'offline_gameparty_stg':        'hadoop fs -cat /dw/stage/offline_gameparty/%(date)s/%(date)s* | wc -l',
            'poker_tbl_info':               'hadoop fs -cat /dw/stage/poker_tbl_info/%(date)s/%(date)s* | wc -l',
            'gameparty_settlement_stg':     'hadoop fs -cat /dw/stage/gameparty_settlement/%(date)s/%(date)s* | wc -l',
            'user_order_stg':               'hadoop fs -cat /dw/stage/user_order/%(date)s/%(date)s* | wc -l',
            'user_generate_order_stg':      'hadoop fs -cat /dw/stage/user_generate_order/%(date)s/%(date)s* | wc -l',
            'feed_clicked_stg':             'hadoop fs -cat /dw/stage/feed_clicked/%(date)s/%(date)s* | wc -l',
            'feed_send_stg':                'hadoop fs -cat /dw/stage/feed_send/%(date)s/%(date)s* | wc -l',
            'push_send_stg':                'hadoop fs -cat /dw/stage/push_send/%(date)s/%(date)s* | wc -l',
            'push_rece_stg':                'hadoop fs -cat /dw/stage/push_rece/%(date)s/%(date)s* | wc -l',
            'push_edit_stg':                'hadoop fs -cat /dw/stage/push_edit/%(date)s/%(date)s* | wc -l',
            'user_daytime_stg':             'hadoop fs -cat /dw/stage/user_daytime/%(date)s/%(date)s* | wc -l',
            'role_created_stg':             'hadoop fs -cat /dw/stage/role_created/%(date)s/%(date)s* | wc -l',
            'user_upgrade_stg':             'hadoop fs -cat /dw/stage/user_grade/%(date)s/%(date)s* | wc -l',
            'user_vip_stg':                 'hadoop fs -cat /dw/stage/user_vip/%(date)s/%(date)s* | wc -l',
            'mission_spark_stg':            'hadoop fs -cat /dw/stage/mission_spark/%(date)s/%(date)s* | wc -l',
            'mission_quit_stg':             'hadoop fs -cat /dw/stage/mission_quit/%(date)s/%(date)s* | wc -l',
            'goods_balance_stg':            'hadoop fs -cat /dw/stage/goods_balance/%(date)s/%(date)s* | wc -l',
            'game_qa_vote_stg':             'hadoop fs -cat /dw/stage/game_qa_vote/%(date)s/%(date)s* | wc -l',
            'pay_page_stg':                 'hadoop fs -cat /dw/stage/pay_page/%(date)s/%(date)s* | wc -l',
            'user_bank_stage':              'hadoop fs -cat /dw/stage/user_bank/%(date)s/%(date)s* | wc -l',
            'gp_uiopen_stg':                'hadoop fs -cat /dw/stage/gp_uiopen/%(date)s/%(date)s* | wc -l',
            'gp_pay_stg':                   'hadoop fs -cat /dw/stage/gp_pay/%(date)s/%(date)s* | wc -l',
            'pay_order_info_stg':           'hadoop fs -cat /dw/stage/pay_order_info/%(date)s/%(date)s* | wc -l',
            'update_order_info_stg':        'hadoop fs -cat /dw/stage/update_order_info/%(date)s/%(date)s* | wc -l',
            'payment_stream_stg':           'hadoop fs -cat /dw/stage/pay_stream/%(date)s/%(date)s* | wc -l',
            'lobby_launch_stg':             'hadoop fs -cat /dw/stage/lobby_launch/%(date)s/%(date)s* | wc -l',
            'lobby_download_stg':           'hadoop fs -cat /dw/stage/lobby_download/%(date)s/%(date)s* | wc -l',
            'lobby_game_download_stg':      'hadoop fs -cat /dw/stage/lobby_game_download/%(date)s/%(date)s* | wc -l',
            'fd_user_channel_stg':          'hadoop fs -cat /dw/stage/user_channel/%(date)s/%(date)s* | wc -l',
            # 中间表
            'active_user_mid':              "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'channel_market_new_reg_mid':   "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'channel_market_new_start_mid': "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'finished_gameparty_uid_mid':   "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'pay_user_mid':                 "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'payment_stream_mid':           "hadoop fs -cat /dw/stage/%(table_name)s/dt=%(date)s/00* | wc -l",
            'pb_gamecoins_stream_mid':      "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'user_dim':                     "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'user_gameparty_info_mid':      "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'user_pay_info':                "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'gameparty_uid_playcnt_mid':    "hadoop fs -ls /dw/stage/%(table_name)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'user_gamecoins_balance_mid':   "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs /data/dist/hive/bin/hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' ",
            'channel_market_payment_mid':   "hadoop fs -cat /dw/stage/%(table_name)s/dt=%(date)s/00* | wc -l",
            'channel_market_reg_mid':       "hadoop fs -cat /dw/stage/%(table_name)s/dt=%(date)s/00* | wc -l",
            'channel_market_active_mid':    "hadoop fs -cat /dw/stage/%(table_name)s/dt=%(date)s/00* | wc -l",
            # "hadoop fs -ls /dw/stage/%(table_name)s/dt=%(date)s | awk '{print $NF}' | grep dw| xargs hive --orcfiledump | grep Rows | awk -F: '{s+=$NF}END{print s}' "
        }
        path = table_path_dict.get(table_name, None)
        return path


    def get_oracle_date_feild_name(self, table_name):
        date_feild_dict = {
            'channel_market_new_reg_mid'    :'fsignup_at',
            'pay_order_info_stg'            :'pstarttime',
            'user_signup_stg'               :'fsignup_at',
            'user_login_stg'                :'flogin_at',
            'user_logout_stg'               :'flogout_at',
            'pb_gamecoins_stream_stg'       :'lts_at',
            'pb_props_stream_stg'           :'lts_at',
            'pb_gifts_stream_stg'           :'lts_at',
            'user_bankrupt_stg'             :'frupt_at',
            'day_user_balance_stg'          :'lts_at',
            'finished_gameparty_stg'        :'fls_at',
            'user_order_stg'                :'forder_at',
            'user_generate_order_stg'       :'forder_at',
            'feed_clicked_stg'              :'fclick_at',
            'feed_send_stg'                 :'ffeed_at',
            'role_created_stg'              :'fcreate_at',
            'user_upgrade_stg'              :'fgrade_at',
            'user_vip_stg'                  :'fvip_at',
            'mission_spark_stg'             :'fspark_at',
            'mission_quit_stg'              :'fquit_at',
            'payment_stream_stg'            :'fdate',
            'active_user_mid'               :'fdate',
            'finished_gameparty_uid_mid'    :'fdate',
            'pay_user_mid'                  :'ffirst_pay_at',
            'pb_gamecoins_stream_mid'       :'fdate',
            'user_dim'                      :'fsignup_at',
            'user_gameparty_info_mid'       :'fdate',
            'user_pay_info'                 :'fpay_at',
            'user_gamecoins_balance_mid'    :'fdate',
            'channel_market_payment_mid'    :'fdate',
            'channel_market_reg_mid'        :'fsignup_at',
            'channel_market_active_mid'     :'fdate',
        }
        date_feild = date_feild_dict.get(table_name, 'flts_at')
        return date_feild


    def get_oracle_table_rows(self, table_name):
        date_feild = self.get_oracle_date_feild_name(table_name)
        sql_dict = {'table_name':table_name, 'date':self.date, 'date_feild':date_feild}

        # 注册用户中间表特殊处理
        if table_name == 'user_dim':
            sql_dict['db'] = 'analysis'
        else:
            sql_dict['db'] = 'stage'

        # 牌局中间表特殊处理
        if table_name == "gameparty_uid_playcnt_mid":
            sql = """ select count(1) from stage.gameparty_uid_tmp  """
        else:
            sql = """ select count(1) from  %(db)s.%(table_name)s where %(date_feild)s >= to_date('%(date)s', 'yyyy-mm-dd')
                  and %(date_feild)s < to_date('%(date)s', 'yyyy-mm-dd') + 1 """ % sql_dict

        datas = self.query_one(self.oracle_conn, sql)
        return datas

    def get_hive_table_rows(self, table_name):
        table_path = self.get_hive_data_path(table_name)
        rows = 0
        if not table_path:
            return 0

        command_str = table_path % {"date":self.date, "table_name":table_name}
        out, err = ShellExec().execute( command_str )

        if err and err.lower().count('error') > 0:
            self.err_io.write( "shell error:%s\n" % err)
            return rows
        try:
            rows = int(out)
        except Exception, e:
            self.err_io.write( "trace:")
            traceback.print_exc(file=self.err_io)
        return rows


    def start_compare(self, table_name):
        # print table_name
        # print 'get oracle data...'
        write_log(table_name)
        write_log("get oracle data...")

        oracle_rows_num = self.get_oracle_table_rows(table_name)

        # print 'get hive data...'
        write_log("get hive data...")

        hive_rows_num = self.get_hive_table_rows(table_name)
        differ = oracle_rows_num - hive_rows_num

        # 将结果写入缓存
        self.write_io.write("%s,%s,%s,%s\n" % (table_name, oracle_rows_num, hive_rows_num, differ) )
        # 每次计算完一个表，就将最新的比较数据刷新到文件中
        self.write_io_data_to_file()

        # 将结果插入数据库中
        self.write_database_rows(table_name, oracle_rows_num, hive_rows_num, differ)


    def write_io_data_to_file(self):
        self.write_io.flush()
        write_file = open('stg_oracle_hive_compare_%s.csv' % self.date, 'w+')
        write_file.write( self.write_io.getvalue() )
        write_file.close()

        self.err_io.flush()
        err_file = open('stg_oracle_hive_compare_err.log', 'w+')
        err_file.write( self.err_io.getvalue() )
        err_file.close()


    def run(self):
        open_file = open(self.table_list_file, 'r')
        self.write_io.write("table_name,oracle_stg_rows,hive_stg_rows,differ\n")
        for line in open_file.readlines():
            table_name = line.strip('\r\n').strip('\n').strip(' ')
            if table_name.startswith('--'):
                continue;
            self.start_compare(table_name)

        open_file.close()
        self.write_io_data_to_file()


    def write_database_rows(self, table_name, oracle_rows_num, hive_rows_num, differ):
        """
        """
        cursor = self.oracle_conn.cursor()

        sql = """delete analysis.dc_source_table_data_info where fdate=date'%s' and
        table_name='%s'""" % (self.date, table_name)
        cursor.execute(sql)

        sql = """insert into analysis.dc_source_table_data_info
            ( fdate, table_name, oracle_rows_num, hive_rows_num, differ_row_num)
            values ( date'%s', '%s', %s, %s, %s)
            """ % (self.date, table_name, oracle_rows_num, hive_rows_num, differ)
        cursor.execute(sql)

        self.oracle_conn.commit()


# 中断信号处理函数 ctrl + C
def sigint_signal_handler(signum, frame):
    print 'protecting the data'
    global comare_stg

    if comare_stg:
        comare_stg.write_io_data_to_file()

    exit(0)


if __name__ == "__main__":
    # 程序愉快地开始啦
    date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")

    if len(sys.argv) == 2:
        file_name = sys.argv[1]
    elif len(sys.argv) == 3:
        file_name = sys.argv[1]
        date = sys.argv[2]
    else:
        print "compare_oracle_stg_data <file_name>"
        print "compare_oracle_stg_data <file_name> <date>"
        sys.exit()

    f_log = open('compare_oracle_stg_data.log', 'w+')

    if f_log is None:
        print "open file(compare_oracle_stg_data.log) is failed!"
        sys.exit()


    # start = datetime.datetime.now()
    # print "start at:" + str(start)
    write_log("start")

    dsn = cx_Oracle.makedsn('210.5.191.175', '1521', 'boyaadw1')
    oracle_conn = cx_Oracle.connect('stage','stage', dsn)

    hive_config = {'host':'192.168.0.94', 'port':10000, 'authMechanism':'PLAIN',
                'user':'hadoop', 'password':'bydchadoop', 'database': 'analysis'}


    hive_conn = pyhs2.connect(host=hive_config['host'], port=hive_config['port'], user=hive_config['user'],
                              password=hive_config['password'], database=hive_config['database'],
                              authMechanism=hive_config['authMechanism'])

    #这里是绑定信号处理函数，将SIGINT绑定在函数sigint_signal_handler上面
    signal.signal(signal.SIGINT, sigint_signal_handler)

    comare_stg = CompareSTG(oracle_conn, hive_conn, file_name, date)
    comare_stg.run()

    # end = datetime.datetime.now()
    # print "ended at:" + str(end)
    # print "cost time:" + str(end-start)
    write_log("finish")

    f_log.close()
