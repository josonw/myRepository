# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

from libs.ImpalaSql import ImpalaSql

class agg_dfqp_user_portrait(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1
        impala = ImpalaSql(host='192.168.0.96')
        hql = """
			invalidate metadata;
			insert overwrite table veda.dfqp_user_portrait select * from veda.dfqp_user_portrait_history where dt = '""" + self.stat_date + """';
			invalidate metadata;
			"""
        res = impala.exe_sql(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_user_portrait(sys.argv[1:])
a()
