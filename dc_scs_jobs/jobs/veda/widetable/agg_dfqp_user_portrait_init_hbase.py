# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_dfqp_user_portrait_init_hbase(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
drop table if exists veda.tmp_veda_user_info_hbase;
create table veda.tmp_veda_user_info_hbase as 
select * from veda.veda_user_info_all;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_user_portrait_init_hbase(sys.argv[1:])
a()
