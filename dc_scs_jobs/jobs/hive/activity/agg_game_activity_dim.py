#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BasePGStat,get_stat_date


class agg_game_activity_actid_dim(BasePGStat):
    """ 游戏活动概要数据维度表"""
    def stat(self):
        """ 重要部分，统计内容 """

        sql = """
        insert into dcnew.game_activity_actid_dim
        (fsk, fgamefsk, fact_id, fact_name, fdis_name, fdesc, fsdate, fedate)
        select  0 fsk, a.fgamefsk, a.fact_id, a.fact_name, a.fact_name fdis_name,
                null fdesc,
                null fsdate,
                null fedate
        from (select a.fgamefsk, a.fact_id, max(a.fact_name) fact_name
                from dcnew.game_activity a
               where fdate = date'%(ld_begin)s'
               group by a.fgamefsk, a.fact_id
              ) a
        left join dcnew.game_activity_actid_dim b
         on a.fgamefsk=b.fgamefsk
        and a.fact_id=b.fact_id
        where b.fact_id is null
        group by a.fgamefsk, a.fact_id, a.fact_name;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """
        insert into dcnew.game_activity_ruleid_dim
        (fsk, fgamefsk, fact_id, frule_id, fdis_name)
        select  0 fsk, a.fgamefsk, a.fact_id, a.frule_id, a.frule_id fdis_name
        from (select fgamefsk, fact_id, frule_id
                from dcnew.game_activity_hour a
               where fdate = date'%(ld_begin)s'
               and frule_id != '-21379' 
               group by a.fgamefsk, a.fact_id, frule_id
              ) a
        left  join dcnew.game_activity_ruleid_dim b
          on a.fgamefsk = b.fgamefsk
         and a.fact_id = b.fact_id
         and a.frule_id = b.frule_id
        where b.fact_id is null
        group by a.fgamefsk, a.fact_id, a.frule_id;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)




if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_game_activity_actid_dim(stat_date)
    a()
