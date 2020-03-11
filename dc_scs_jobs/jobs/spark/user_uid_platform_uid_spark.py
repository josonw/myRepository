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

-- fbpid:fuid 作为key
CREATE EXTERNAL TABLE analysis.user_uid_platform_uid_hbase
 (key string,
 fplatform_uid string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping" = ":key, f:fplatform_uid")
TBLPROPERTIES ("hbase.table.name" = "user_uid_platform_uid");
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
        """ select fbpid, fuid, fplatform_uid
            from stage.user_async_stg
            where dt = '%(date)s' """,
    ]

    # hbase_table, hbase_key, hive_sql
    sql_info_list = [("user_uid_platform_uid", ["fbpid", "fuid"], sql % {'date':date} ) for sql in  sql_list]

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

    spark_app_name = "user_uid_platform_uid"
    hbase_col_family = 'f:'
    sql_list = get_sql_list(date)

    start_spark_run(spark_app_name, hbase_col_family, sql_list)


if __name__ == '__main__':
    main()
