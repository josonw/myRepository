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

class load_match_user_wide_using(BaseStatModel):
    def create_tab(self):
        """ 比赛用户中间表_宽表专用 """
        hql = """create table if not exists dim.match_user_wide_using
            (
             fbpid         varchar(50)     comment  'bpid',
             fgame_id      bigint          comment  '子游戏id',
             fuid          bigint          comment  '用户id',
             join_amt      bigint          comment  '比赛报名费总额（人民币）',
             max_join_amt  bigint          comment  '比赛最高报名费总额（人民币）',
             join_cnt      bigint          comment  '比赛报名次数',
             match_cnt     bigint          comment  '比赛参加次数（比赛牌局数）',
             join_match    bigint          comment  '报名比赛数',
             match_match   bigint          comment  '参加比赛数',
             win_cnt       bigint          comment  '比赛获奖次数',
             win_amt       bigint          comment  '比赛获奖总额（人民币）',
             max_win_amt   bigint          comment  '比赛最高获奖总额（人民币）'
            )
            partitioned by (dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分,统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.match_user_wide_using partition (dt = "%(statdate)s" )
          select a.fbpid,
                 coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                 fuid,
                 sum(join_amt) join_amt,
                 sum(max_join_amt) max_join_amt,
                 sum(join_cnt) join_cnt,
                 sum(match_cnt) match_cnt,
                 sum(join_match) join_match,
                 sum(match_match) match_match,
                 sum(win_cnt) win_cnt,
                 sum(win_amt) win_amt,
                 sum(max_win_amt) max_win_amt
            from (--报名参加牌局比赛
                  select jg.fbpid,
                         coalesce(jg.fgame_id,cast (0 as bigint)) fgame_id,
                         jg.fuid,
                         0 join_amt,
                         0 max_join_amt,
                         count(1) join_cnt,
                         0 match_cnt,
                         count(distinct fmatch_id) join_match,
                         0 match_match,
                         0 win_cnt,
                         0 win_amt,
                         0 max_win_amt
                    from stage.join_gameparty_stg jg
                   where jg.dt='%(statdate)s'
                     and coalesce(fmatch_id,'0') <>'0'
                   group by fbpid,fgame_id,fuid
                   union all
                --参赛
                  select ug.fbpid,
                         coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                         ug.fuid,
                         0 join_amt,
                         0 max_join_amt,
                         0 join_cnt,
                         count(1) match_cnt,
                         0 join_match,
                         count(distinct fmatch_id) match_match,
                         0 win_cnt,
                         0 win_amt,
                         0 max_win_amt
                    from stage.user_gameparty_stg ug
                   where ug.dt='%(statdate)s'
                     and coalesce(fmatch_id,'0')<>'0'
                   group by fbpid,fgame_id,fuid
                   union all
                --比赛场发放消耗
                  select fbpid,
                         coalesce(mg.fgame_id,cast (0 as bigint)) fgame_id,
                         fuid,
                         sum(case when fact_id = '1' then fcost else 0 end) join_amt,
                         max(case when fact_id = '1' then fcost else 0 end) max_join_amt,
                         0 join_cnt,
                         0 match_cnt,
                         0 join_match,
                         0 match_match,
                         sum(case when fact_id = '2' then 1 else 0 end) win_cnt,
                         max(case when fact_id = '2' then fcost else 0 end) win_amt,
                         max(case when fact_id = '2' then fcost else 0 end) max_win_amt
                    from stage.match_economy_stg mg
                   where mg.dt='%(statdate)s'
                     and coalesce(fmatch_id,'0')<>'0'
                   group by fbpid,fgame_id,fuid
                 ) a
           group by fbpid,
                    fgame_id,
                    fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_match_user_wide_using(sys.argv[1:])
a()
