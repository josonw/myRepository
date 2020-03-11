# coding: utf-8

import sys
import datetime
from pyspark import SparkContext
from pyspark.sql import HiveContext

# in the lib.zip
from util import getHbasePool

"""
--  建表语句
ADD JAR /usr/local/dist/hive/lib/hive-hbase-handler-1.2.1.jar;
ADD JAR /usr/local/dist/hive/lib/zookeeper-3.4.6.jar;
ADD JAR /usr/local/dist/hive/lib/guava-14.0.1.jar;
ADD JAR /opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.36/lib/hive/lib/hbase-client.jar;
ADD JAR /opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.36/lib/hive/lib/hbase-common.jar;
ADD JAR /opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.36/lib/hive/lib/hbase-hadoop-compat.jar;
ADD JAR /opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.36/lib/hive/lib/hbase-hadoop2-compat.jar;
ADD JAR /opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.36/lib/hive/lib/hbase-protocol.jar;
ADD JAR /opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.36/lib/hive/lib/hbase-server.jar;
ADD JAR /opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.36/lib/hive/lib/htrace-core.jar;
-- fbpid         varchar(50), 作为key
-- fuid          bigint,
CREATE EXTERNAL TABLE analysis.user_full_hbase
 (key string,
 fplatform_uid varchar(50),
 fsignup_at    string,
 fname         varchar(200),
 fdisplay_name varchar(400),
 fgender       bigint,
 fgame_coin    decimal(20,2),
 fby_coin      decimal(20,2),
 fpoint        decimal(20,2),
 fip           varchar(20),
 flanguage     varchar(50),
 fcountry      varchar(50),
 fcity         varchar(50),
 fage          bigint,
 fmail         varchar(100),
 fbirthday     string,
 fmarriage     decimal(10,2),
 fschool_record  varchar(50),
 fuuid           bigint,
 fgrade          bigint, -- 注册表没有的字段
 ffriends_num    bigint,
 fappfriends_num bigint,
 fentrance_id    bigint,
 fversion_info   varchar(50),
 flastlogin_at   string, -- 注册表没有的字段
 flastpay_at     string, -- 注册表没有的字段
 flastsync_at    string, -- 注册表没有的字段
 fad_code        varchar(50),
 fsource_path    varchar(100),
 fm_dtype        varchar(100),
 fm_pixel        varchar(100),
 fm_os           varchar(100),
 fm_network      varchar(100),
 fm_operator     varchar(100),
 fm_imei         varchar(100)

)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping" = ":key,b:fplatform_uid, b:fsignup_at, b:fname, b:fdisplay_name, b:fgender, b:fgame_coin, b:fby_coin, b:fpoint, b:fip, b:flanguage, b:fcountry, b:fcity, b:fage, b:fmail, b:fbirthday, b:fmarriage, b:fschool_record, b:fuuid, b:fgrade, b:ffriends_num, b:fappfriends_num, b:fentrance_id, b:fversion_info, b:flastlogin_at, b:flastpay_at, b:flastsync_at, b:fad_code, b:fsource_path, b:fm_dtype, b:fm_pixel, b:fm_os, b:fm_network, b:fm_operator, b:fm_imei")
TBLPROPERTIES ("hbase.table.name" = "user_full");
"""


def put_into_hbase(part, table_name, batch_size=1000):
    """
    part: rdd partition
    table_name: hbase table name
    batch_size: 批处理的数量，默认1000
    """
    pool = getHbasePool()
    with pool.connection() as conn:
        table = conn.table(table_name)
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


def restruct(d, col_family, keys, null_chars=[None, ""]):
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


def filter_null(d, null_fields=[], null_chars=[None, "", 0, "0"]):
    """
    d: dict
    null_fields: list 如果是空值（null, "", 0, "0"）需要清除的字段
    null_chars: list 属于空值的字符，默认`[None, "", 0, "0"]`
    """
    if not null_fields:
        null_fields = d.keys()

    for field in null_fields:
        if d.get(field) in null_chars:
            return False
    return True


def yestoday():
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


def get_sql_list(date):
    sql_list = [
        # 注册信息
        """ select
            fbpid, fuid,
            max(fplatform_uid)      fplatform_uid,
            max(fsignup_at)         fsignup_at,
            max(fgender)            fgender,
            max(fip)                fip,
            max(flanguage)          flanguage,
            max(fcountry)           fcountry,
            max(fcity)              fcity,
            max(fage)               fage,
            max(ffriends_num)       ffriends_num,
            max(fappfriends_num)    fappfriends_num,
            max(fentrance_id)       fentrance_id,
            max(fversion_info)      fversion_info,
            max(fad_code)           fad_code,
            max(fsource_path)       fsource_path,
            max(fm_dtype)           fm_dtype,
            max(fm_pixel)           fm_pixel,
            max(fm_os)              fm_os,
            max(fm_network)         fm_network,
            max(fm_operator)        fm_operator,
            max(fm_imei)            fm_imei
            from stage.user_signup_stg
            where dt = '%(date)s'
            group by fbpid, fuid """,

        # 登陆信息
        """ select fbpid, fuid, max(fplatform_uid) fplatform_uid
              from stage.user_login_stg
             where dt='%(date)s'
             group by fbpid, fuid """,

        # 等级信息
        """
        -- 一个用户一天里等级有很多级，有很多垃圾数据，所以取最后一条
        -- 等级大于300的当垃圾处理
        select fbpid, fuid, flevel fgrade
        from
        (
            select fbpid,
                fuid,
                flevel,
                row_number() over(partition by fbpid, fuid order by uus.fgrade_at desc, flevel desc) as rowcn
            from stage.user_grade_stg uus
            where uus.dt = '%(date)s'
            and nvl(flevel,9999) < 300
        ) a
        where rowcn = 1
        """,
    ]

    # hbase_table, hbase_key, hive_sql
    sql_info_list = [("user_full", ["fbpid", "fuid"], sql % {'date':date} ) for sql in  sql_list]

    return sql_info_list


def start_spark_run(spark_app_name, hbase_col_family, sql_list):
    sc = SparkContext(appName=spark_app_name)
    hiveContext = HiveContext(sc)

    for table, rowkey_field, sql in sql_list:
        df = hiveContext.sql(sql)
        df.map( lambda row: row.asDict() ) \
          .filter( lambda d: filter_null(d, null_fields=rowkey_field) ) \
          .map( lambda d: restruct(d, hbase_col_family, rowkey_field) ) \
          .foreachPartition( lambda part: put_into_hbase(part, table) )


def main():
    if len(sys.argv) < 2:
        date = yestoday()
    elif len(sys.argv) == 2:
        date = sys.argv[1]
    else:
        print "Usage: %s [date]" % argv[0]
        sys.exit(1)

    spark_app_name = "user_full_update"
    hbase_col_family = 'b:'
    sql_list = get_sql_list(date)

    start_spark_run(spark_app_name, hbase_col_family, sql_list)


if __name__ == '__main__':
    main()
