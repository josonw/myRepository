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


class agg_fangzuobi_suspect_user_part8(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
-- c1:取当日游客，判断过往30天是否有在不同设备上登录
insert overwrite table veda.suspect_user_c1  partition(dt='%(statdate)s')
select m.fuid
  from (select distinct fm_imei, fuid
          from stage.user_login_stg x
          left join dim.bpid_map y
            on x.fbpid = y.fbpid
         where x.dt >= date_add('%(statdate)s', -29)
           and x.dt <= '%(statdate)s'
           and y.fgamename = '地方棋牌'
           and x.fentrance_id = 1
           and x.fuid in (select fuid
                            from stage.user_login_stg a
                            left join dim.bpid_map b
                              on a.fbpid = b.fbpid
                           where a.dt = '%(statdate)s'
                             and b.fgamename = '地方棋牌'
                             and a.fentrance_id = 1)
       ) m
  left join (select fuid from veda.suspect_user_c1) n
    on m.fuid = n.fuid
 where n.fuid is null
 group by m.fuid
having count(*) >= 2
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- c3:手机账号一周内在≥N台设备上登录，N暂定为5
insert overwrite table veda.suspect_user_c3 partition(dt='%(statdate)s')
select m.fuid
  from (select distinct fm_imei, fuid
          from stage.user_login_stg x
          left join dim.bpid_map y
            on x.fbpid = y.fbpid
         where x.dt >= date_add('%(statdate)s', -6)
           and x.dt <= '%(statdate)s'
           and y.fgamename = '地方棋牌'
           and x.fentrance_id = 1002
           and x.fuid in (select fuid
                            from stage.user_login_stg a
                            left join dim.bpid_map b
                              on a.fbpid = b.fbpid
                           where a.dt = '%(statdate)s'
                             and b.fgamename = '地方棋牌'
                             and a.fentrance_id = 1002)
       ) m
left join (select fuid from veda.suspect_user_c1) n on m.fuid = n.fuid
where n.fuid is null
group by m.fuid having count(*) >= 5
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_part8(sys.argv[1:])
a()
