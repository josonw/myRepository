#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_bpid_relation(BaseStatModel):

    # 对bpid和游戏做关系处理
    def create_tab(self):
        hql = """
        
		insert overwrite table veda.bpid_relation 
		select '三公-泰语', fbpid, fterminaltypename, fplatformname, fhallname from dim.bpid_map where fgamename = '三公' and fplatformname = '泰语' 
		union all 
		select '海外棋牌-泰国', fbpid, fterminaltypename, fplatformname, fhallname from dim.bpid_map where fgamename = '海外棋牌' and fplatformname = '泰国';
		
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = load_bpid_relation(sys.argv[1:])
a()
