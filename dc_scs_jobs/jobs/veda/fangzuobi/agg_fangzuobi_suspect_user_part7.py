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


class agg_fangzuobi_suspect_user_part7(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
-- b3:隶属于当天注册拥有≥N个账号的设备号，N暂定为10
insert overwrite table veda.suspect_user_b3  partition(dt='%(statdate)s')
select distinct fuid, ftoken, case when substring(fsignup_at, 1, 10) = '%(statdate)s' then 1 else 0 end is_new
  from stage.user_push_stg
 where ftoken in (select ftoken
                   from (select distinct ftoken, fuid
                          from stage.user_push_stg x
                          left join dim.bpid_map y
                            on x.fbpid = y.fbpid
                         where dt = '%(statdate)s'
                           and y.fgamename = '地方棋牌'
                           and x.fpush_platform <> 'boyaa'
                        ) m
                  group by m.ftoken
                 having count(*) >= 10)
 order by ftoken;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_part7(sys.argv[1:])
a()
