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


class agg_match_round_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.match_round_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fparty_type         varchar(10)      comment '牌局类型',
               fpname              varchar(100)     comment '一级场次',
               fsubname            varchar(100)     comment '二级场次',
               fgsubname           varchar(100)     comment '三级场次',
               fjoin_unum          bigint           comment '报名人数',
               fjoin_num           bigint           comment '报名人次',
               fplay_unum          bigint           comment '参赛人数',
               fplay_num           bigint           comment '参赛人次',
               fparty_num          bigint           comment '牌局数',
               fjoin_fee           bigint           comment '报名费',
               fwin_unum           bigint           comment '进奖圈人数',
               fwin_num            bigint           comment '进奖圈人次',
               fvictory_unum       bigint           comment '决胜局人数',
               fvictory_num        bigint           comment '决胜局人次',
               fwin_gold           bigint           comment '奖励发放金条',
               fwin_fare           bigint           comment '奖励发放话费券',
               fvictory_gold       bigint           comment '决胜局奖励金条',
               fvictory_silver     bigint           comment '决胜局奖励银币',
               frelieve_gold       bigint           comment '复活消耗金条',
               frelieve_card       bigint           comment '复活消耗复活卡',
               frelieve_unum       bigint           comment '复活人数',
               frelieve_num        bigint           comment '复活人次',
               fround_gold         bigint           comment '轮间奖励金条',
               fround_silver       bigint           comment '轮间奖励银币'
               )comment '快速赛复活相关信息'
               partitioned by(dt date)
        location '/dw/bud_dm/match_round_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fparty_type', 'fpname', 'fsubname', 'fgsubname'],
                            'groups':[[1, 1, 1, 1],
                                      [0, 0, 0, 0] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取牌局相关指标
            drop table if exists work.match_round_info_tmp_1_%(statdatenum)s;
          create table work.match_round_info_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t1.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t1.fpname,'%(null_str_report)s')  fpname
                 ,coalesce(t1.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t1.fgsubname,'%(null_str_report)s')  fgsubname
                 ,t1.fuid
                 ,ftbl_id
                 ,finning_id
            from dim.match_gameparty t1  --牌局
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and coalesce(fmatch_id,'0')<>'0'
             and fsubname = '快速赛';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fpname,'%(null_str_group_rule)s')  fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s')  fsubname
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,0 fjoin_unum
                 ,0 fjoin_num
                 ,count(distinct fuid) fplay_unum
                 ,count(fuid) fplay_num
                 ,count(distinct concat_ws('0', finning_id, ftbl_id)) fparty_num
                 ,0 fjoin_fee
                 ,0 fwin_unum
                 ,0 fwin_num
                 ,0 fvictory_unum
                 ,0 fvictory_num
                 ,0 fwin_gold
                 ,0 fwin_fare
                 ,0 fvictory_gold
                 ,0 fvictory_silver
                 ,0 frelieve_gold
                 ,0 frelieve_card
                 ,0 frelieve_unum
                 ,0 frelieve_num
                 ,0 fround_gold
                 ,0 fround_silver
            from work.match_round_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fpname
                    ,fsubname
                    ,fgsubname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            drop table if exists work.match_round_info_tmp_%(statdatenum)s;
          create table work.match_round_info_tmp_%(statdatenum)s as
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取报名相关指标
            drop table if exists work.match_round_info_tmp_2_%(statdatenum)s;
          create table work.match_round_info_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t1.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t1.fpname,'%(null_str_report)s')  fpname
                 ,coalesce(t1.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t1.fgsubname,'%(null_str_report)s')  fgsubname
                 ,t1.fuid
                 ,fentry_fee
            from dim.join_gameparty t1  --报名
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and coalesce(fmatch_id,'0')<>'0'
             and fparty_type = '快速赛';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fpname,'%(null_str_group_rule)s')  fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s')  fsubname
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,count(distinct fuid) fjoin_unum
                 ,count(fuid) fjoin_num
                 ,0 fplay_unum
                 ,0 fplay_num
                 ,0 fparty_num
                 ,sum(fentry_fee) fjoin_fee
                 ,0 fwin_unum
                 ,0 fwin_num
                 ,0 fvictory_unum
                 ,0 fvictory_num
                 ,0 fwin_gold
                 ,0 fwin_fare
                 ,0 fvictory_gold
                 ,0 fvictory_silver
                 ,0 frelieve_gold
                 ,0 frelieve_card
                 ,0 frelieve_unum
                 ,0 frelieve_num
                 ,0 fround_gold
                 ,0 fround_silver
            from work.match_round_info_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fpname
                    ,fsubname
                    ,fgsubname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            insert into table work.match_round_info_tmp_%(statdatenum)s
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取奖励相关指标
            drop table if exists work.match_round_info_tmp_3_%(statdatenum)s;
          create table work.match_round_info_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t2.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t2.fgsubname,'%(null_str_report)s')  fgsubname
                 ,coalesce(t2.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t2.fpname,'%(null_str_report)s')  fpname
                 ,t1.fuid
                 ,fitem_id
                 ,fact_id
                 ,fitem_num
                 ,frank
                 ,t2.fwin_num
                 ,t2.fvictory_num
            from stage.match_economy_stg t1  --奖励
            left join dim.match_config t2
              on concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) = t2.fmatch_id
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and (coalesce(t1.fmatch_cfg_id,0)<>0 or coalesce(t1.fmatch_log_id,0)<>0)
             and t1.fsubname = '快速赛'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fpname,'%(null_str_group_rule)s')  fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s')  fsubname
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,0 fjoin_unum
                 ,0 fjoin_num
                 ,0 fplay_unum
                 ,0 fplay_num
                 ,0 fparty_num
                 ,0 fjoin_fee
                 ,count(distinct case when frank > 0 and frank <= fwin_num then fuid end) fwin_unum
                 ,count(case when frank > 0 and frank <= fwin_num then fuid end) fwin_num
                 ,count(distinct case when frank > 0 and frank <= fvictory_num then fuid end) fvictory_unum
                 ,count(case when frank > 0 and frank <= fvictory_num then fuid end) fvictory_num
                 ,sum(case when frank > 0 and frank <= fwin_num and fitem_id = '1' then fitem_num end) fwin_gold
                 ,sum(case when frank > 0 and frank <= fwin_num and fitem_id = '200007' then fitem_num end) fwin_fare
                 ,sum(case when frank > 0 and frank <= fvictory_num and fitem_id = '1' then fitem_num end) fvictory_gold
                 ,sum(case when frank > 0 and frank <= fvictory_num and fitem_id = '200007' then fitem_num end) fvictory_silver
                 ,sum(case when fitem_id = '1' and fact_id = '5' then fitem_num end) frelieve_gold  --复活金条
                 ,sum(case when fitem_id = '150' and fact_id = '5' then fitem_num end) frelieve_card  --复活券
                 ,count(distinct case when fact_id = '5' and fitem_num > 0 then fuid end) frelieve_unum
                 ,count(case when fact_id = '5' and fitem_num > 0  then fuid end) frelieve_num
                 ,sum(case when fact_id = '3' and fitem_id = '1' then fitem_num end) fround_gold       --胜局奖励
                 ,sum(case when fact_id = '3' and fitem_id = '0' then fitem_num end) fround_silver     --胜局奖励
            from work.match_round_info_tmp_3_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fpname
                    ,fsubname
                    ,fgsubname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            insert into table work.match_round_info_tmp_%(statdatenum)s
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 插入目标表
        hql = """  insert overwrite table bud_dm.match_round_info  partition(dt='%(statdate)s')
                   select "%(statdate)s" fdate
                          ,fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fgame_id
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,fparty_type
                          ,fpname
                          ,fsubname
                          ,fgsubname
                          ,sum(fjoin_unum) fjoin_unum
                          ,sum(fjoin_num) fjoin_num
                          ,sum(fplay_unum) fplay_unum
                          ,sum(fplay_num) fplay_num
                          ,sum(fparty_num) fparty_num
                          ,sum(fjoin_fee) fjoin_fee
                          ,sum(fwin_unum) fwin_unum
                          ,sum(fwin_num) fwin_num
                          ,sum(fvictory_unum) fvictory_unum
                          ,sum(fvictory_num) fvictory_num
                          ,sum(fwin_gold) fwin_gold
                          ,sum(fwin_fare) fwin_fare
                          ,sum(fvictory_gold) fvictory_gold
                          ,sum(fvictory_silver) fvictory_silver
                          ,sum(frelieve_gold) frelieve_gold
                          ,sum(frelieve_card) frelieve_card
                          ,sum(frelieve_unum) frelieve_unum
                          ,sum(frelieve_num) frelieve_num
                          ,sum(fround_gold) fround_gold
                          ,sum(fround_silver) fround_silver
                     from work.match_round_info_tmp_%(statdatenum)s
                    group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                             ,fparty_type
                             ,fpname
                             ,fsubname
                             ,fgsubname;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.match_round_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.match_round_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.match_round_info_tmp_3_%(statdatenum)s;
                 drop table if exists work.match_round_info_tmp_4_%(statdatenum)s;
                 drop table if exists work.match_round_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_match_round_info(sys.argv[1:])
a()
