# -*- coding: UTF-8 -*-
import sys
import datetime
import commands

# 缺省日期参数
stat_date = str()
if len(sys.argv) == 1:
    stat_date = str(datetime.date.today()-datetime.timedelta(days=1))
else:
    args = sys.argv[1].split(',')
    stat_date = args[0]

# 执行Spark计算
commands.getstatusoutput("ssh hadoop@10.30.101.216 -p 3600 \"/data/veda/spark-2.2.0-bin-hadoop2.7/code_and_data/Hive_and_Spark_task.sh %s\"" % stat_date)
