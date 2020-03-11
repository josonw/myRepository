#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import re
import datetime
import codecs
import signal
from cStringIO import StringIO

import psycopg2
import commands

import cx_Oracle
import pymongo

import traceback

reload(sys)
sys.setdefaultencoding('utf-8')

#解决Oracle乱码问题
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

#类型转换
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

#全局变量
main_control = None
DEBUG        = False

class DBManage:
    def __init__(self, oracle_conn, pg_conn):
        self.oracle_conn = oracle_conn
        self.pg_conn = pg_conn
        self.sql_output = open('sql_output.sql','a')

    def __del__(self):
        self.sql_output.close()

    def _query_datas(self, conn, sql):
        cur = conn.cursor()
        datas = []
        if DEBUG:
            self.sql_output.write(sql)
            self.sql_output.write('\n')

        try:
            cur.execute( sql )
            column_names = [ d[0] for d in cur.description ]
            datas = cur.fetchall()
        except Exception, e:
            conn.rollback()
            raise e
        finally:
            cur.close()

        return datas, column_names

    def query_oracle_datas(self, sql):
        datas = []
        column_names = []
        try:
            datas, column_names = self._query_datas(self.oracle_conn, sql)
        except Exception, e:
            raise e

        return datas, column_names

    def query_pg_datas(self, sql):
        datas = []
        column_names = []
        try:
            datas, column_names = self._query_datas(self.pg_conn, sql)
        except Exception, e:
            raise e

        return datas

class GetDiffAndWriteFileControl:
    def __init__(self, table_owner, table_name, orcle_records, pg_records):
        self.table_owner    = table_owner
        self.table_name     = table_name

        # oracle记录行数
        self.orcle_records = orcle_records
        self.pg_records    = pg_records

        # 行数统计
        self.total_line = 0
        self.right_line = 0

        self.write_file_io = StringIO()

        # show flag
        self.is_show_none_diff= False

    def get_table_compare_result_info(self):
        info = {'table_owner'   :self.table_owner,
                'table_name'    :self.table_name,
                'orcle_records' :self.orcle_records,
                'pg_records'    :self.pg_records,
                'total'         :self.total_line,
                'right'         :self.right_line
                }
        return info


    def write_tabel_title(self, column_names):
        self.write_file_io.write(u'db_type,')
        for column in column_names:
            self.write_file_io.write( u"%s," % column  )
        self.write_file_io.write(u'\n')

    def is_write_or_pg_diff_to_file(self, diff_dict):
        flag = True
        value_list = diff_dict.get('v', [])

        if self.is_show_none_diff :
            pass
        else:
            try:
                result = reduce(lambda x, y: x+y, value_list)
                if result == 0:
                    flag = False
                    self.right_line += 1
            except Exception, e:
                pass
        return flag

    def get_or_pg_diff_final_and_write_file(self, key_list, column_names, oracle_final, pg_final):
        self.write_tabel_title(column_names)

        for key in key_list:
            or_v = oracle_final.get(key, [])
            pg_v = pg_final.get(key, [])

            self.total_line += 1


            or_result = or_v
            try:
                or_result = reduce(lambda x, y: x+y, or_v)
                if or_result == 0:
                    or_result = []
            except Exception, e:
                pass

            pg_result = pg_v
            try:
                pg_result = reduce(lambda x, y: x+y, pg_v)
                if pg_result == 0:
                    pg_result = []
            except Exception, e:
                pass

            if not or_result and not pg_result:
                self.right_line +=1
                continue


            differ_v = self.get_differ_or_pg(or_v, pg_v)

            or_dict =   {'tb_n':self.table_name, 'db_f':'or',    'k':key, 'v':or_v}
            pg_dict =   {'tb_n':self.table_name, 'db_f':'pg',    'k':key, 'v':pg_v}
            diff_dict = {'tb_n':self.table_name, 'db_f':'diff',  'k':key, 'v':differ_v}

            if self.is_write_or_pg_diff_to_file( diff_dict ):
                self.write_or_pg_diff_to_file(or_dict, pg_dict, diff_dict, len(column_names[1:]))

        write_file = codecs.open("output/"+self.table_name+'.csv', encoding='utf-8', mode='w+')
        write_file.write( self.write_file_io.getvalue() )
        write_file.close()

    def get_or_v_pg_v(self, datas):
        or_v = []
        pg_v = []
        for row in datas:
            if row.get('db_f', '') == 'or':
                or_v = row.get('v', [])
            elif row.get('db_f', '') == 'pg':
                pg_v = row.get('v', [])
        return or_v, pg_v

    def get_differ_or_pg(self, or_v, pg_v):
        v_len = len(or_v)
        differ_v = []

        for i, feild in enumerate(or_v):
            # 将oracle中的None值转换为0方便比对
            feild = feild if feild else 0
            try:
                pg_v[i]
                pass
            except Exception, e:
                differ_v.append('false')
                continue

            # 将pg中的None值转换为0方便比对
            pg_v[i] = pg_v[i] if pg_v[i] else 0
            # oracle字段值为非数字
            if type(feild) == str or type(feild) == unicode:
                if feild == pg_v[i]:
                    differ_v.append(0)
                else:
                    differ_v.append('false')
            else:
                # oracle为数字，但pg字段不为数字
                if type(pg_v[i]) == str or type(pg_v[i]) == unicode:
                    differ_v.append( 'false' )
                else:
                    differ_v.append( round(feild-pg_v[i], 2) )
        return differ_v


    def write_or_pg_diff_to_file(self, or_dict, pg_dict,diff_dict, data_col_len ):
        # data_col_len = len( or_dict.get('v', []) )

        for temp_dict in [or_dict, pg_dict, diff_dict]:
            self.write_each_feild(temp_dict, data_col_len)


    def write_each_feild(self, db_dict, col_len):
        self.write_file_io.write( u"%s," % db_dict.get('db_f', '') )
        self.write_file_io.write( u"%s," % db_dict.get('k', '') )

        value_list = db_dict.get('v', [])
        for i in range(col_len):
            try:
                value = value_list[i]
            except Exception, e:
                value = 'false'

            # 如果值为None用0替代
            value = value if value else 0
            self.write_file_io.write( u"%s," % value )

        self.write_file_io.write(u'\n' )


class MainControl:
    def __init__(self, oracle_conn, pg_conn, date, input_table_name=None):
        self.date               = date
        self.db_manage          = DBManage(oracle_conn, pg_conn)
        self.input_table_name   = input_table_name

        self.error_file_io = StringIO()

        # 显示差异为0的对比结果
        self.is_show_none_diff= False

        # 用来保存各个表的总体比对情况
        self.global_compare_info = []

    def write_global_compare_info(self):
        global_file_io = StringIO()

        global_file_io.write(u"table_owner,table_name,orcle_records,pg_records,total_lines,right_lines,wrong_rate(%)\n")
        for table_info in self.global_compare_info:
            table_owner     = table_info.get('table_owner','')
            table_name      = table_info.get('table_name', '')
            orcle_records   = table_info.get('orcle_records', '')
            pg_records      = table_info.get('pg_records', '')
            total           = table_info.get('total', 0)
            right           = table_info.get('right', 0)

            wrong_rate  = round( (total - right) * 100 / total, 2 ) if total else 0
            global_file_io.write( u"%s,%s,%s,%s,%s,%s,%s\n" % (table_owner, table_name, orcle_records, pg_records, total, right, wrong_rate) )

        global_file = codecs.open("output/"+self.date + '_global_compare_info.csv', encoding='utf-8', mode='w+')
        global_file.write( global_file_io.getvalue() )
        global_file.close()

    def write_sql_error_log(self, table_name, sql, sql_error_type, e):
        self.error_file_io.write( u"table_name==>%s, error_type==>%s sql error\n" % (table_name, sql_error_type ) )
        self.error_file_io.write( u"error_info==>%s" % e )
        self.error_file_io.write( u"sql==>%s\n" % sql )

    def datas_to_result(self, datas):
        final = {}
        key_list = []
        for row in datas:
            value = list( row[1:] )
            key_list.append( row[0] )
            final[ row[0] ] = value

        return key_list, final

    def db_datas_to_final(self, datas):
        key_list, final = self.datas_to_result(datas)
        return key_list, final

    def get_key_list(self, or_table_keys_list, pg_table_keys_list):
        key_list = or_table_keys_list[:]
        key_list.extend( pg_table_keys_list )
        key_list = list(set(key_list))
        return key_list

    def get_one_table_compare_result(self, table_owner, table_name, oracle_sql, pg_sql):

        print table_name + ' get oracle data ing...'
        try:
            oracle_datas, column_names= self.db_manage.query_oracle_datas(oracle_sql)
        except Exception, e:
            self.write_sql_error_log(table_name, oracle_sql, 'oracle', e)
            raise e

        print table_name + ' get pg data ing...'
        try:
            pg_datas = self.db_manage.query_pg_datas(pg_sql)
        except Exception, e:
            self.write_sql_error_log(table_name, pg_sql, 'pg', e)
            raise e

        try:

            or_table_keys_list, oracle_final = self.db_datas_to_final(oracle_datas)
            pg_table_keys_list, pg_final = self.db_datas_to_final(pg_datas)

            key_list = self.get_key_list(or_table_keys_list, pg_table_keys_list)

            diff_control =  GetDiffAndWriteFileControl(table_owner, table_name, len(or_table_keys_list), len(pg_table_keys_list))
            diff_control.is_show_none_diff = self.is_show_none_diff

            print table_name + ' compare the diff and write to file ing...'
            diff_control.get_or_pg_diff_final_and_write_file(key_list, column_names, oracle_final, pg_final)

            result_info = diff_control.get_table_compare_result_info()
            self.global_compare_info.append( result_info )

        except Exception, e:
            traceback.print_exc(file=self.error_file_io)
            raise e


    def get_all_table_list(self):
        sql_condition = ""
        if self.input_table_name:
            sql_condition = " where ftable_name = '%s' " % self.input_table_name

        table_list_sql = """ select ftable_name, foracle_sql, fpg_sql, fowner
                            from data_compare_table_sql %s """ % sql_condition

        datas, column_names= self.db_manage.query_oracle_datas(table_list_sql)
        return datas

    def run(self):
        sql_dict = {'date':self.date}
        table_sql_list = self.get_all_table_list()

        for row in table_sql_list:
            table_name = row[0]
            oracle_sql = row[1]
            pg_sql     = row[2]
            table_owner= row[3]

            oracle_sql = oracle_sql % sql_dict
            pg_sql = pg_sql % sql_dict

            try:
                self.get_one_table_compare_result(table_owner, table_name, oracle_sql, pg_sql)
            except Exception, e:
                print e
                continue

        error_file = codecs.open("output/sql_error.log",encoding='utf-8', mode='w+')
        error_file.write( self.error_file_io.getvalue() )
        error_file.close()

        # write global info
        self.write_global_compare_info()


def get_input_args():
    date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    table_name = None

    if len(sys.argv) == 1:
        print "usage: compare_oracle_pg_detail 2014-12-01"
        print "usage: compare_oracle_pg_detail table_name 2014-12-01 "
        exit(0)
    elif len(sys.argv) == 2:
        if len(sys.argv[1]) == 10:
            date = sys.argv[1]
        else:
            print "date format error"
            exit(0)
    elif len(sys.argv) == 3:
        table_name = sys.argv[1]
        date = sys.argv[2]
        if len(date) != 10:
            print "date format error"
            exit(0)
    else:
        print "usage: compare_oracle_pg_detail 2014-12-01"
        print "usage: compare_oracle_pg_detail table_name 2014-12-01 "
        exit(0)
    return date, table_name

# 中断信号处理函数 ctrl + C
def sigint_signal_handler(signum, frame):
    print 'protecting the data'
    global main_control

    if main_control:
        main_control.write_global_compare_info()

    exit(0)

if __name__ == "__main__":
    date, input_table_name = get_input_args()
    print "compare the data of " + date

    start = datetime.datetime.now()
    print "start at:" + str(start)

    # oracle连接
    dsn = cx_Oracle.makedsn('210.5.191.175', '1521', 'boyaadw1')
    oracle_conn = cx_Oracle.connect('ANALYSIS','ANALYSIS', dsn)

    if sys.platform == 'win32':
        # 本地连接pg
        pg_conn = psycopg2.connect(host='175.45.5.236', database='boyaadw', user='analysis', password='analysis', port=5432)
    else:
        # 服务器连接pg
        pg_conn = psycopg2.connect(host='192.168.0.126', database='boyaadw', user='analysis', password='analysis', port=5432)

    main_control = MainControl(oracle_conn, pg_conn, date, input_table_name)

    #这里是绑定信号处理函数，将SIGINT绑定在函数sigint_signal_handler上面
    signal.signal(signal.SIGINT, sigint_signal_handler)

    # let's rock!
    main_control.run()

    # 多个表才压缩，单个表不进行压缩
    if not input_table_name:
        # 压缩文件
        os.system("zip %s_compare_result.zip output/*" % date)

    end = datetime.datetime.now()
    print "ended at:" + str(end)
    print "cost time:" + str(end-start)
