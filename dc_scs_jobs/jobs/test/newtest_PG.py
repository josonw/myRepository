#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStatModel import BasePGStat
from PublicFunc import PublicFunc
import service.sql_const as sql_const


class test(BasePGStat):
    """ PG执行存储过程运算 """
    def stat(self):
        sql = """
            select * from  dcnew.bpid_map
            limit 10
        """
        self.append(sql)

        self.exe_hql_list()

    #生成统计实例
a = test(sys.argv[1:])
a()

##END
