#-*- coding: UTF-8 -*-
#!/usr/local/python272/bin/python
import os
import sys
import re
import datetime

import psycopg2
import pyhs2
import commands

import cx_Oracle

from PublicFunc import PublicFunc

class CompareTableRows():
    def __init__(self, file_name, date):

        dsn = cx_Oracle.makedsn('210.5.191.175', '1521', 'boyaadw1')

        #pg_连接
        self.pg_conn = psycopg2.connect(host='192.168.0.126', database='boyaadw', user='analysis', password='analysis', port=5432)

        # oracle连接
        self.oracle_conn = cx_Oracle.connect('ANALYSIS','ANALYSIS', dsn)

        self.file_name = file_name
        self.date      = date

        #时间参数字典
        self.date_dict = self.get_date_value_dict()

        self.write_file_name = 'compare_out_'+ self.date +'.csv'

    def get_date_value_dict(self):
        yesterday = PublicFunc.add_days(self.date, -1)
        ld_month_begin = PublicFunc.trunc(self.date, "MM")
        ld_1month_ago_begin = PublicFunc.add_months(ld_month_begin, -1)
        ld_2month_ago_begin = PublicFunc.add_months(ld_month_begin, -2)
        ld_1month_after_begin = PublicFunc.add_months(ld_month_begin, 1)
        ld_week_begin = PublicFunc.trunc(self.date, "IW")

        date_dict = {   'date'                  :self.date,
                        'yesterday'             :yesterday,
                        'ld_month_begin'        :ld_month_begin,
                        'ld_1month_ago_begin'   :ld_1month_ago_begin,
                        'ld_2month_ago_begin'   :ld_2month_ago_begin,
                        'ld_1month_after_begin' :ld_1month_after_begin,
                        'ld_week_begin'         :ld_week_begin}
        return date_dict


    def start_compare(self, write_file, table_name):

        condition_entry = SQLCondition(table_name, self.date_dict)
        sql_dict = condition_entry.get_sql_condition()

        # 上面的get_sql_condition 会修改表名
        worked_table_name = condition_entry.table_name
        print "++++++++++++++++++" + worked_table_name + "++++++++++++++++++"

        select_date = condition_entry.get_select_date_value()

        data_rows_entry = DBRows(sql_dict, self.pg_conn, self.oracle_conn)
        hive_rows_num, pg_rows_num, oracle_rows_num = data_rows_entry.get_data_rows()


        out = worked_table_name + "," + select_date + "," + str(hive_rows_num)
        out += "," + str(pg_rows_num) + "," + str(oracle_rows_num)
        out += "," + str(hive_rows_num-pg_rows_num) + "," + str(oracle_rows_num-pg_rows_num)
        out += '\n'
        write_file.write(out)


    def read_table_list_from_file(self):
        open_file = open(self.file_name, 'r')

        write_file = open(self.write_file_name, 'w')

        #添加表头
        write_file.write("table_name,date,hive_rows,pg_rows,oracle_rows,hive_pg_differ,oracle_pg_differ\n")

        for line in open_file.readlines():
            table_name = line.strip('\r\n').strip('\n').strip(' ')
            if table_name.startswith('--'):
                continue;
            self.start_compare(write_file, table_name)

        open_file.close()
        write_file.close()


class DBRows():
    def __init__(self, sql_dict, pg_conn, oracle_conn):
        self.sql_dict    = sql_dict

        self.pg_conn     = pg_conn
        self.oracle_conn = oracle_conn

    def query_datas(self, conn, sql):
        cur = conn.cursor()
        datas = []
        try:
            cur.execute( sql )
            datas = cur.fetchall()
        except Exception, e:
            print e
            conn.rollback()
        finally:
            cur.close()
        return datas


    def query_one(self, conn, sql):
        # 建立Cursor对象
        cur = conn.cursor()

        datas = (0,)

        # 插叙数据，并获取结果
        try:
            cur.execute( sql )
            datas = cur.fetchone()
        except Exception, e:
            print e
            conn.rollback()
        finally:
            cur.close()

        return datas[0]


    def get_oracle_table_rows(self):
        sql = """ select count(1) from  analysis.%(oracle_sql_contion)s """ % self.sql_dict
        datas = self.query_one(self.oracle_conn, sql)
        return datas

    def get_pg_table_rows(self):
        sql = """ select count(1) from analysis.%(pg_sql_contion)s """ % self.sql_dict
        datas = self.query_one(self.pg_conn, sql)
        return datas

    def get_hive_table_rows(self):
        out = 0
        command_str = 'hadoop fs -cat /dw/analysis/%(hive_sql_contion)s/00* | wc -l' % self.sql_dict
        lines = commands.getoutput( command_str )
        try:
            out = int(lines)
        except Exception, e:
            print e
        return out


    def get_data_rows(self):
        hive_rows_num   =   self.get_hive_table_rows()
        pg_rows_num     =   self.get_pg_table_rows()
        oracle_rows_num =   self.get_oracle_table_rows()

        return hive_rows_num, pg_rows_num, oracle_rows_num

# 获取查询条件
class SQLCondition():
    def __init__(self, table_name, date_dict):
        self.table_name = table_name
        self.is_not_partition_flag = False

        self.date_dict = date_dict

        # 判断表是否是全局表
        self.is_not_partition_table()


    def get_table_date_field(self):
        table_date_dict = {
            'ddz_user_login_info_new'       :'flogin_at',
            'marketing_channel_test_data'   :'flts_at',
            'marketing_roi_cost_fct'        :'fsignup_month',
            'pay_center_final_order_fct'    :'forder_date',
            'pay_center_normal_order_fct'   :'forder_date',
            'pay_center_repair_cheat_fct'   :'flts_at',
            'pay_center_unnormal_order_fct' :'flts_at',
            'register_retain_pay_fct'       :'fsignup_at',
            'dc_payuser_coin_stream'        :'fsdate',
            'marketing_roi_retain_pay'      :'fsignup_month',
        }
        date_field = table_date_dict.get(self.table_name, 'fdate')
        return date_field

    # 判断是否是非分区表
    def is_not_partition_table(self):
        # not_partition_table_list = ['marketing_forecast_show_fct', 'offline_gameparty_all_fct']
        if self.table_name.count('-a-'):
            self.is_not_partition_flag = True
            self.table_name = self.table_name.strip('-a-')

    # 获取特殊比较数据比较时间例如，递延比较上个月收入
    def get_date_match_value(self):
        date_match_dict = {
            "by_deferred_gamecoin_fct"      :"ld_1month_ago_begin",
            "by_deferred_goods_balance_fct" :"ld_1month_ago_begin",
            "by_deferred_bycoin_fct"        :"ld_1month_ago_begin",
            "by_deferred_income_fct"        :"ld_1month_ago_begin",
            "marketing_roi_cost_fct"        :"ld_month_begin",
            "marketing_cpc_day_cost_info"   :"ld_month_begin",
            "marketing_roi_cps_cost_fct"    :"ld_month_begin",
            "user_pay_num_retained_fct"     :"yesterday",
        }
        date_match = date_match_dict.get(self.table_name, 'date')
        return date_match


    def get_select_date_value(self):
        if self.is_not_partition_flag:
            date_value = 'all table'
        else:
            date_match = self.get_date_match_value()
            date_match_value = "%%(%s)s" % date_match
            date_value = date_match_value % self.date_dict

        return date_value


    def get_date_match_condition(self):
        date_match = self.get_date_match_value()

        oracle_sql_contion = """%%(table_name)s where %%(date_field)s = to_date('%%(%s)s', 'yyyy-mm-dd') """ % date_match
        pg_sql_contion = """%%(table_name)s where %%(date_field)s = DATE'%%(%s)s' """ % date_match
        hive_sql_contion ="""%%(table_name)s/dt=%%(%s)s""" % date_match

        return oracle_sql_contion, pg_sql_contion, hive_sql_contion


    def get_sql_condition(self):
        pg_sql_contion = "%(table_name)s"
        oracle_sql_contion = "%(table_name)s"
        hive_sql_contion = "%(table_name)s"


        date_field = self.get_table_date_field()
        sql_dict = {'table_name':self.table_name, 'date_field':date_field}

        sql_dict.update( self.date_dict )

        if not self.is_not_partition_flag:
            oracle_sql_contion, pg_sql_contion, hive_sql_contion = self.get_date_match_condition()

        oracle_sql_contion = oracle_sql_contion % sql_dict
        pg_sql_contion = pg_sql_contion % sql_dict
        hive_sql_contion = hive_sql_contion % sql_dict

        sql_dict['oracle_sql_contion']  =   oracle_sql_contion
        sql_dict['pg_sql_contion']      =   pg_sql_contion
        sql_dict['hive_sql_contion']    =   hive_sql_contion
        return sql_dict


if __name__ == "__main__":
    date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")

    if len(sys.argv) == 2:
        file_name = sys.argv[1]
    elif len(sys.argv) == 3:
        file_name = sys.argv[1]
        date = sys.argv[2]
    else:
        print "compare_hive_pg_multi_table_rows file_name"
        print "compare_hive_pg_multi_table_rows file_name date"
        exit(0)

    entry = CompareTableRows(file_name, date)
    entry.read_table_list_from_file()