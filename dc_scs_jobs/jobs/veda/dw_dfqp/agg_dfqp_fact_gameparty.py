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


class agg_dfqp_fact_gameparty(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
			insert overwrite table dw_dfqp.fact_gameparty
			partition( dt="%(statdate)s" )
			select 
				dt, 
				fuid mid, 
				fbpid, 
				fsubname subgame_room_id, 
				count(1) total_party_num, 
				sum(case when fgamecoins > 0 then 1 else 0 end) win_party_num, 
				sum(case when fgamecoins < 0 then 1 else 0 end) lose_party_num, 
				sum(nvl(fgamecoins, 0)) total_amt, 
				sum(case when fgamecoins > 0 then nvl(fgamecoins, 0) else 0 end) win_amt, 
				sum(case when fgamecoins < 0 then nvl(fgamecoins, 0) else 0 end) lose_amt,  
				sum(case when nvl(unix_timestamp(fe_timer) - unix_timestamp(fs_timer), 0) > 3600 then 0 else nvl(unix_timestamp(fe_timer) - unix_timestamp(fs_timer), 0) end) play_time, 
				sum(nvl(cast(fcharge as bigint), 0)) charge 
			from 
				stage_dfqp.user_gameparty_stg 
			where 
				dt = '%(statdate)s' and nvl(fmatch_id, '0') = '0' 
			group by dt, fuid, fbpid, fsubname;
	
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_fact_gameparty(sys.argv[1:])
a()
