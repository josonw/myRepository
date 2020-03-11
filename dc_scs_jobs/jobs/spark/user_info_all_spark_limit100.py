# -*- coding: utf:8 -*-
# Author: AnsenWen
from pyspark import SparkContext, HiveContext
import sys
import os
import datetime
from util import getHbasePool
reload(sys)
sys.setdefaultencoding('utf-8')

HBASE_TABLE = "user_info_all"
APP_NAME = "user_info"

def get_sql_list(date):
    sql_list = []
    date_dict = {'ld_day':date}

    # 注册信息
    # Ansen:国家、省份、城市的信息改在ip_country,ip_province, ip_city字段获取
    sql = """ select fbpid, fuid,
                     max(fsignup_at)      fsignup_at,
                     max(fchannel_code)   fchannel_code,
                     max(fm_dtype)        fm_dtype,
                     max(fm_pixel)        fm_pixel,
                     max(fm_imei)         fm_imei,
                     max(fip)             fip,
                     max(fip_country)     fip_country,
                     max(fip_province)    fip_province,
                     max(fip_city)        fip_city
            from (
                select * from stage.user_signup_stg
                 where dt = '%(ld_day)s'
                 limit 100
                 ) t
            group by fbpid, fuid """ % date_dict
    sql_list.append(sql)

    # 登录信息
    sql = """ select fbpid, fuid,
                   max(ffirst_date) ffirst_log_date,
                   max(flast_date) flast_log_date,
                   max(flogin_num) flogin_num,
                   max(flogin_day) flogin_day
              from (select * from stage.user_login_all
                 where dt = '%(ld_day)s'
                 limit 100) t
             where dt='%(ld_day)s'
             group by fbpid, fuid """ %  date_dict
    sql_list.append(sql)

    # 活跃信息
    sql = """ select fbpid, fuid,
                     min(ffirst_act_date)   ffirst_act_date,
                     max(flast_act_date)    flast_act_date,
                     max(fact_day)          fact_day
              from (select * from stage.user_active_all
                 where dt = '%(ld_day)s'
                 limit 100) t
             where dt='%(ld_day)s'
             group by fbpid, fuid """ %  date_dict
    sql_list.append(sql)

    # 破产信息
    sql = """ select fbpid, fuid,
                     min(ffirst_rupt_date)   ffirst_rupt_date,
                     max(flast_rupt_date)    flast_rupt_date,
                     max(fbankrupt_cnt)      fbankrupt_cnt
              from (select * from stage.user_bankrupt_all
                     where dt = '%(ld_day)s'
                     limit 100) t
             where dt='%(ld_day)s'
             group by fbpid, fuid """ %  date_dict
    sql_list.append(sql)

    # 破产救济信息
    sql = """ select fbpid, fuid,
                     min(ffirst_relieve_date)   ffirst_relieve_date,
                     max(flast_relieve_date)    flast_relieve_date,
                     max(frelieve_cnt)          frelieve_cnt,
                     max(fgamecoins)            frelieve_gamecoins
               from (select * from stage.user_bankrupt_relieve_all
                      where dt='%(ld_day)s'
                      limit 100) t
              group by fbpid, fuid """ %  date_dict
    sql_list.append(sql)

    # 牌局信息
    sql = """ select fbpid, fuid,
                     min(ffirst_play_date)   ffirst_play_date,
                     max(flast_play_date)    flast_play_date,
                     max(fcharge)            fcharge,
                     max(fplay_inning)       fplay_inning,
                     max(fplay_time)         fplay_time,
                     max(fplay_day)          fplay_day,
                     max(fwin_inning)        fwin_inning,
                     max(flose_inning)       flose_inning,
                     max(fwin_gamecoins)     fwin_gamecoins,
                     max(flose_gamecoins)    flose_gamecoins
              from (select * from stage.user_gameparty_all
                     where dt='%(ld_day)s'
                     limit 100) t
             group by fbpid, fuid """ %  date_dict
    sql_list.append(sql)

    # 金币信息
    sql = """ select fbpid, fuid,
                     min(ffirst_act_date)   ffirst_act_gc_date,
                     max(flast_act_date)    flast_act_gc_date,
                     max(fgamecoins_num)    fgamecoins_num
                from (select * from stage.user_gamecoins_all
                       where dt='%(ld_day)s'
                       limit 100) t
               group by fbpid, fuid """ %  date_dict
    sql_list.append(sql)

    # 付费信息
    sql = """ select fbpid, fuid,
                     min(ffirsttime) ffirst_pay_time,
                     max(flasttime)  flast_pay_time,
                     max(fusd)       fusd,
                     max(fnum)       fpay_num
              from (select * from stage.payment_stream_all
                       where dt='%(ld_day)s'
                       limit 100) t
             group by fbpid, fuid """ %  date_dict
    sql_list.append(sql)

    return sql_list

def yesterday():
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

# 将key里含有空字符或者为0的过滤掉
def filter_key_null(d, null_fields=[], null_chars=[None, "", 0, "0"]):
    """
    d: dict
    null_fields: list 如果是空值（null, "", 0, "0"）需要清除的字段
    null_chars: list 属于空值的字符，默认`[None, "", 0, "0"]`
    """
    if null_fields is not None:
        null_fields = d.keys()

    for field in null_fields:
        if d.get(field) in null_chars:
            return False
    return True

# 重新调整列族的结构
def restruct_col_family(d, col_family, keys, null_chars=[None, ""]):
    """
    d: dict
    keys: list 作为rowkey的字段
    col_family: 列族(eg: i: )
    null_chars: list 于空值的字符，默认`[None, ""]`
    """
    rowkey = ":".join([unicode(d.get(k)).encode("utf-8") for k in keys])

    nd = {(col_family + k): unicode(v).encode("utf-8")
          for k, v in d.iteritems() if k not in keys and v not in null_chars}
    return (rowkey, nd)


# 批量插入HBase
def put_into_hbase(part, label_name, batch_size=1000):
    """
    part: rdd partition
    table_name: hbase table name
    batch_size: 批处理的数量，默认1000
    """
    pool = getHbasePool()
    with pool.connection() as conn:
        table = conn.table(HBASE_TABLE)
        batch = table.batch()
        counter = 0
        for record in part:
            if not record or len(record) != 2:
                continue
            else:
                batch.put(record[0], record[1])
                counter += 1
            if counter % batch_size == 0:
                batch.send()
        batch.send()

def put_data_from_hive_to_hbase(sql_list, hc, col_family):
    rowkey = ["fbpid", "fuid"]
    dtypes_list = []
    for sql in sql_list:
        df = hc.sql(sql)
        dt = filter(lambda d: d[0] not in rowkey and d not in dtypes_list, df.dtypes)
        dtypes_list.extend(dt)
        df.map(lambda row: row.asDict()) \
          .filter(lambda d: filter_key_null(d, null_fields=rowkey)) \
          .map(lambda d: restruct_col_family(d, col_family, rowkey)) \
          .foreachPartition(lambda part: put_into_hbase(part, HBASE_TABLE))
    return dtypes_list

# shell命令获取hive表的列名
def get_hive_col():
    shell_str = """ echo "describe analysis.user_info_all_hbase;" | hive"""
    result = os.popen(shell_str).readlines()
    hive_col = map(lambda r: r.split('\t')[0].strip(),\
            filter(lambda r: 'hive' not in r and 'key' not in r , result))
    return hive_col

# 比较hbase与hive表的列，如果不同，则重新建表
def map_hbase_to_hive(dtypes_list, col_family):
    hive_col = get_hive_col()

    hive_dtypes_list = []
    hbase_col = []
    hbase_col_with_family = []
    for d in dtypes_list:
        hbase_col.append(d[0])
        hive_dtypes_list.append(d[0] + "  " + d[1])

    for c in hbase_col:
        if c not in hive_col:
            hbase_col_str = ', '.join([col_family + c for c in hbase_col])
            hive_dtypes_str = ', '.join(hive_dtypes_list)
            hive_dict = {"hive_dtypes": hive_dtypes_str, "hbase_col": hbase_col_str}
            return create_hive_table(hive_dict)


def create_hive_table(hive_dict):
    # shell_drop = """echo "drop table if exists analysis.user_info_all_hbase; " | hive"""
    shell_drop = """hive -e "drop table if exists analysis.user_info_all_hbase;" """
    os.system(shell_drop)
    sql = """create external table if not exists analysis.user_info_all_hbase
            (   key string,
                %(hive_dtypes)s
            )
            stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
            with serdeproperties
            ("hbase.columns.mapping" = ":key, %(hbase_col)s")
            tblproperties ("hbase.table.name" = "user_info_all");
         """ % hive_dict
    print sql
    shell_create = """echo '%s' | hive""" % sql
    os.system(shell_create)

if __name__ == '__main__':
    stime = datetime.datetime.now()
    sc = SparkContext(appName=APP_NAME)
    hc = HiveContext(sc)
    date = yesterday()
    # 统一一个列族，避免多个列族下字段重复
    col_family = "c:"
    sql_list = get_sql_list(date)
    dtypes_list = put_data_from_hive_to_hbase(sql_list, hc, col_family)
    print dtypes_list
    map_hbase_to_hive(dtypes_list, col_family)
    etime = datetime.datetime.now()
    print "Succeed! The running time is " + str((etime - stime).seconds) + " seconds."