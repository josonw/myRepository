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


class agg_fangzuobi_suspect_user_part4(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
-- b1:隶属于当天注册拥有≥N个账号的IP，N暂定为20
insert overwrite table veda.suspect_user_b1 partition(dt='%(statdate)s')
select a.fuid, a.fip, case when o.fuid is null then 0 else 1 end isnull
  from (select distinct fuid, fip
         from stage.user_login_stg i
         left join dim.bpid_map j
           on i.fbpid = j.fbpid
        where i.dt = '%(statdate)s'
          and j.fgamename = '地方棋牌'
          and i.fip in (select fip
                          from (select distinct fip, fuid
                                  from stage.user_login_stg x
                                  left join dim.bpid_map y
                                    on x.fbpid = y.fbpid
                                 where x.dt = '%(statdate)s'
                                   and y.fgamename = '地方棋牌'
                               ) m
                         group by m.fip
                        having count(*) >= 20)
       ) a
  left join (select fuid
               from stage.user_signup_stg p
               left join dim.bpid_map q
                 on p.fbpid = q.fbpid
              where p.dt = '%(statdate)s'
                and q.fgamename = '地方棋牌'
            ) o
    on a.fuid = o.fuid
 where a.fuid is not null ;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_part4(sys.argv[1:])
a()
