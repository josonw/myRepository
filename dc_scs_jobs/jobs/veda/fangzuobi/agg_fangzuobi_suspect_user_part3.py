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


class agg_fangzuobi_suspect_user_part3(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """-- a1:嫌疑时间段注册，嫌疑时间暂定为凌晨1:00-6:00
insert overwrite table veda.suspect_user_a1  partition(dt='%(statdate)s')
select fuid, fsignup_at
  from stage.user_signup_stg x
  left join dim.bpid_map y
    on x.fbpid = y.fbpid
 where y.fgamename = '地方棋牌'
   and x.dt = '%(statdate)s'
   and x.fsignup_at >= concat('%(statdate)s',' 01:00:00')
   and x.fsignup_at <= concat('%(statdate)s',' 05:59:59')
 order by fsignup_at;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_part3(sys.argv[1:])
a()
