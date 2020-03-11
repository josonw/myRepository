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


class agg_fzb_relieve_user_tag_day(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
insert overwrite table veda.dfqp_fzb_relieve_user_tag_day 
partition(dt='%(statdate)s')
select 
	x.fuid mid, 
	coalesce(y.gold_play_time, 0) gold_play_time, 
	coalesce(y.total_play_time, 0) total_play_time, 
	coalesce(y.100_play_amt, 0) 100_play_amt, 
	case when y.100_play_amt < 0 then 1 else 0 end is_100_lose, 
	coalesce(z.pay_sum, 0.00) pay_sum, 
	coalesce(z.relieve_count, 0) relieve_count 
from 
(select distinct fuid from dim.user_act x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where fgamename = '地方棋牌' and fhallname not in ('香港大厅', '台湾大厅') and dt = '%(statdate)s') x
left join 
(select dt, fuid, sum(case when fgame_id in (204, 3, 10, 21, 23, 903, 27, 29) then fplay_time else 0 end) gold_play_time, sum(fplay_time) total_play_time, 
sum(case when split(fsubname, '_')[1] = '100' then fwin_amt - flose_amt else 0 end) 100_play_amt 
from dim.user_gameparty y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where fgamename = '地方棋牌' and fhallname not in ('香港大厅', '台湾大厅') 
and y1.dt = '%(statdate)s' 
group by dt, fuid) y on x.fuid = y.fuid
left join 
(select fuid, pay_sum, relieve_count from stage_dfqp.user_action_everyday where dt = '%(statdate)s') z on x.fuid = z.fuid

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_relieve_user_tag_day(sys.argv[1:])
a()
