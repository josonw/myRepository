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


class agg_fangzuobi_suspect_user_part9(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
-- d1:嫌疑时段在金流游戏活跃X时长。暂定X=90分钟，嫌疑时段为凌晨1:00-6:00，金流游戏为7人焖鸡，斗牛，拖拉机，马股
insert overwrite table veda.suspect_user_d1 partition(dt='%(statdate)s')
select fuid, sum(unix_timestamp(fe_timer) - unix_timestamp(fs_timer)) play_time
  from stage.user_gameparty_stg x
  left join dim.bpid_map y
    on x.fbpid = y.fbpid
 where y.fgamename = '地方棋牌'
   and x.dt = '%(statdate)s'
   and x.fs_timer >= concat('%(statdate)s',' 01:00:00')
   and x.fs_timer <= concat('%(statdate)s',' 05:59:59')
   and x.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903)
 group by fuid
having play_time >= 5400;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """-- d2:有至少Y天日玩牌时长超过正常时间。暂定Y=2，正常时间为600分钟（10小时）
insert overwrite table veda.suspect_user_d2
partition(dt='%(statdate)s')
select fuid, count(*) days, sum(fplay_time)/count(*) avg
  from veda.user_basic_day
 where dt >= date_add('%(statdate)s', -6)
   and dt <= '%(statdate)s'
   and fplay_time >= 36000
 group by fuid
having days >= 2
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_part9(sys.argv[1:])
a()
