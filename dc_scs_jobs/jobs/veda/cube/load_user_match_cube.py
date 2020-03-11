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


class load_user_match_cube(BaseStatModel):

    """地方棋牌赛事多维表
     """
    def create_tab(self):

        hql = """
        create table if not exists veda.user_match_cube
        (
          fhallfsk bigint,
          fgame_id bigint,
          fuid                bigint,              --用户游戏ID
          fparty_type         varchar(100),        --牌局类型
          fpname              varchar(100),        --牌局一级分类名称
          fsubname            varchar(100),        --牌局二级分类名称
          fgsubname           varchar(100),        --牌局三级分类名称
          fentry_fee          bigint            comment '金条报名费',
          fapply_cnt          bigint            comment '报名次数',
          fmatch_cnt          bigint            comment '参赛次数',
          fapplymatch_cnt     bigint            comment '有报名的参赛次数',
          fwin_num            bigint            comment '获奖次数',
          fquit_cnt           bigint            comment '取消报名次数',
          fout_cost           decimal(20,2)     comment '发放成本',
          fin_cost            decimal(20,2)     comment '消耗成本',
          faward_cnt          bigint            comment '进入奖圈次数',
          fvictory_num        bigint            comment '进入决胜圈次数',
          fgold_out           bigint            comment '金条产出',
          fgold_in            bigint            comment '金条回收',
          fbill_out           bigint            comment '发放话费',
          fparty_num          bigint            comment '牌局数',
          fwin_party_num      bigint            comment '胜局数',
          frelieve_num        bigint            comment '复活次数'
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


    def stat(self):

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table veda.user_match_cube
        partition( dt="%(statdate)s" )
       select fhallfsk
              ,fgame_id
              ,fuid
              ,fparty_type
              ,fpname
              ,fsubname
              ,fgsubname
              ,sum(fentry_fee) fentry_fee
              ,sum(fapply_cnt) fapply_cnt
              ,sum(fmatch_cnt) fmatch_cnt
              ,sum(fapplymatch_cnt) fapplymatch_cnt
              ,sum(fwin_num) fwin_num
              ,sum(fquit_cnt) fquit_cnt
              ,sum(fout_cost) fout_cost
              ,sum(fin_cost) fin_cost
              ,sum(faward_cnt) faward_cnt
              ,sum(fvictory_num) fvictory_num
              ,sum(fgold_out) fgold_out
              ,sum(fgold_in) fgold_in
              ,sum(fbill_out) fbill_out
              ,sum(fparty_num) fparty_num
              ,sum(fwin_party_num) fwin_party_num
              ,sum(frelieve_num) frelieve_num
         from (select tt.fhallfsk
                      ,t1.fgame_id
                      ,t1.fuid
                      ,t1.fparty_type
                      ,t1.fpname            --牌局一级分类
                      ,t1.fsubname          --牌局二级分类
                      ,t1.fgsubname         --牌局三级分类
                      ,sum(case when fitem_id = 1 then fentry_fee end) fentry_fee       --金条报名费
                      ,count(1) fapply_cnt
                      ,0 fmatch_cnt
                      ,0 fapplymatch_cnt
                      ,0 fwin_num
                      ,0 fquit_cnt
                      ,0 fout_cost
                      ,0 fin_cost
                      ,0 faward_cnt
                      ,0 fvictory_num
                      ,0 fgold_out
                      ,0 fgold_in
                      ,0 fbill_out
                      ,0 fparty_num
                      ,0 fwin_party_num
                      ,0 frelieve_num
                 from dim.join_gameparty t1
                 join dim.bpid_map_bud tt
                   on t1.fbpid = tt.fbpid
                  and tt.fgamefsk = 4132314431
                where t1.dt = '%(statdate)s'
                group by tt.fhallfsk
                         ,t1.fgame_id
                         ,t1.fuid
                         ,t1.fparty_type
                         ,t1.fpname            --牌局一级分类
                         ,t1.fsubname          --牌局二级分类
                         ,t1.fgsubname         --牌局三级分类

                union all

               select tt.fhallfsk
                      ,t1.fgame_id
                      ,t1.fuid
                      ,t1.fparty_type
                      ,t1.fpname            --牌局一级分类
                      ,t1.fsubname          --牌局二级分类
                      ,t1.fgsubname         --牌局三级分类
                      ,0 fentry_fee       --金条报名费
                      ,0 fapply_cnt
                      ,count(distinct t1.fmatch_id) fmatch_cnt
                      ,count(distinct case when t2.fmatch_id is not null then t1.fmatch_id end) fapplymatch_cnt
                      ,0 fwin_num
                      ,0 fquit_cnt
                      ,0 fout_cost
                      ,0 fin_cost
                      ,0 faward_cnt
                      ,0 fvictory_num
                      ,0 fgold_out
                      ,0 fgold_in
                      ,0 fbill_out
                      ,count(distinct concat_ws('0', ftbl_id, finning_id)) fparty_num
                      ,count(distinct case when fgamecoins > 0 then concat_ws('0', ftbl_id, finning_id) end) fwin_party_num
                      ,0 frelieve_num
                 from dim.match_gameparty t1
                 left join dim.join_gameparty t2
                   on t1.fuid = t2.fuid
                  and t1.fmatch_id = t2.fmatch_id
                  and t2.dt = '%(statdate)s'
                 join dim.bpid_map_bud tt
                   on t1.fbpid = tt.fbpid
                  and tt.fgamefsk = 4132314431
                where t1.dt = '%(statdate)s'
                group by tt.fhallfsk
                         ,t1.fgame_id
                         ,t1.fuid
                         ,t1.fparty_type
                         ,t1.fpname            --牌局一级分类
                         ,t1.fsubname          --牌局二级分类
                         ,t1.fgsubname         --牌局三级分类

                union all

               select tt.fhallfsk
                      ,t1.fgame_id
                      ,t1.fuid
                      ,coalesce(t2.fparty_type,'%(null_str_report)s')  fparty_type
                      ,coalesce(t2.fpname,'%(null_str_report)s')  fpname
                      ,coalesce(t2.fsubname,'%(null_str_report)s')  fsubname
                      ,coalesce(t2.fgsubname,'%(null_str_report)s')  fgsubname
                      ,0 fentry_fee       --金条报名费
                      ,0 fapply_cnt
                      ,0 fmatch_cnt
                      ,0 fapplymatch_cnt
                      ,count(case when fact_id = 2 then fuid end) fwin_num
                      ,0 fquit_cnt
                      ,sum(case when fio_type = 1 then fcost end) fout_cost
                      ,sum(case when fio_type = 2 then fcost end) fin_cost
                      ,count(case when frank > 0 and frank <= fwin_num then fuid end) faward_cnt
                      ,count(case when frank > 0 and frank <= fvictory_num then fuid end) fvictory_num
                      ,sum(case when fio_type = 1 and fitem_id = '1' then fcost end) fgold_out
                      ,sum(case when fio_type = 2 and fitem_id = '1' then fcost end) fgold_in
                      ,sum(case when fio_type = 1 and fitem_id in ('200004','200005','200006','200007','200012','200013','200014','200015','400003') then fcost end) fbill_out
                      ,0 fparty_num
                      ,0 fwin_party_num
                      ,count(case when fact_id = 5 and fitem_num > 0 then fuid end) frelieve_num
                 from stage.match_economy_stg t1
                 left join dim.match_config t2
                   on concat_ws('_', cast (t1.fmatch_cfg_id as string), cast (t1.fmatch_log_id as string)) = t2.fmatch_id
                  and t2.dt = '%(statdate)s'
                 join dim.bpid_map_bud tt
                   on t1.fbpid = tt.fbpid
                  and tt.fgamefsk = 4132314431
                where t1.dt = '%(statdate)s'
                group by tt.fhallfsk
                         ,t1.fgame_id
                         ,t1.fuid
                         ,coalesce(t2.fparty_type,'%(null_str_report)s')
                         ,coalesce(t2.fgsubname,'%(null_str_report)s')
                         ,coalesce(t2.fsubname,'%(null_str_report)s')
                         ,coalesce(t2.fpname,'%(null_str_report)s')

                union all

               select tt.fhallfsk
                      ,t1.fgame_id
                      ,t1.fuid
                      ,coalesce(t2.fparty_type,'%(null_str_report)s')  fparty_type
                      ,coalesce(t2.fpname,'%(null_str_report)s')  fpname
                      ,coalesce(t2.fsubname,'%(null_str_report)s')  fsubname
                      ,coalesce(t2.fgsubname,'%(null_str_report)s')  fgsubname
                      ,0 fentry_fee       --金条报名费
                      ,0 fapply_cnt
                      ,0 fmatch_cnt
                      ,0 fapplymatch_cnt
                      ,0 fwin_num
                      ,count(case when fcause in (1,2) then fuid end) fquit_cnt
                      ,0 fout_cost
                      ,0 fin_cost
                      ,0 faward_cnt
                      ,0 fvictory_num
                      ,0 fgold_out
                      ,0 fgold_in
                      ,0 fbill_out
                      ,0 fparty_num
                      ,0 fwin_party_num
                      ,0 frelieve_num
                 from stage.quit_gameparty_stg t1
                 left join dim.match_config t2
                   on concat_ws('_', cast (t1.fmatch_cfg_id as string), cast (t1.fmatch_log_id as string)) = t2.fmatch_id
                  and t2.dt = '%(statdate)s'
                 join dim.bpid_map_bud tt
                   on t1.fbpid = tt.fbpid
                  and tt.fgamefsk = 4132314431
                where t1.dt = '%(statdate)s'
                group by tt.fhallfsk
                         ,t1.fgame_id
                         ,t1.fuid
                         ,coalesce(t2.fparty_type,'%(null_str_report)s')
                         ,coalesce(t2.fgsubname,'%(null_str_report)s')
                         ,coalesce(t2.fsubname,'%(null_str_report)s')
                         ,coalesce(t2.fpname,'%(null_str_report)s')
            ) t1
        group by t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fuid
                 ,t1.fparty_type
                 ,t1.fpname
                 ,t1.fsubname
                 ,t1.fgsubname
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = load_user_match_cube(sys.argv[1:])
a()
