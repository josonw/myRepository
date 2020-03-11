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


class agg_bud_user_match_gsubgame_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_gsubgame_detail (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fparty_type           varchar(10)      comment '牌局类型',
               fpname                varchar(100)     comment '一级场次',
               fsubname              varchar(100)     comment '二级场次',
               fgsubname             varchar(100)     comment '三级场次',
               fapply_unum           bigint           comment '报名人数',
               fapply_cnt            bigint           comment '报名人次',
               ffirst_apply_unum     bigint           comment '首次报名人数',
               fnew_apply_unum       bigint           comment '新增用户报名人数',
               fapply_fr_unum        bigint           comment '无偿报名人数',
               fapply_co_unum        bigint           comment '有偿报名人数',
               fmatch_unum           bigint           comment '参赛人数',
               fmatch_cnt            bigint           comment '参赛人次',
               ffirst_match_unum     bigint           comment '首次参赛人数',
               fnew_match_unum       bigint           comment '新增用户参赛人数',
               fmatch_fr_unum        bigint           comment '无偿参赛人数',
               fmatch_co_unum        bigint           comment '有偿参赛人数',
               fwin_unum             bigint           comment '获奖人数',
               fwin_num              bigint           comment '获奖人次',
               fbegin_cnt            bigint           comment '开赛次数',
               fbegin_suss_cnt       bigint           comment '开赛成功次数',
               fbegin_fail_cnt       bigint           comment '开赛失败次数',
               ffapply_pay_unum      bigint           comment '当日付费用户首次报名人数',
               ffmatch_pay_unum      bigint           comment '当日付费用户首次参赛人数',
               fapply_pay_unum       bigint           comment '当日付费用户报名人数',
               fapply_pay_cnt        bigint           comment '当日付费用户报名次数',
               fapply_pay_income     decimal(20,2)    comment '当日付费用户报名用户付费金额',
               fmatch_pay_unum       bigint           comment '当日付费用户参赛人数',
               fmatch_pay_cnt        bigint           comment '当日付费用户参赛次数',
               fmatch_pay_income     decimal(20,2)    comment '当日付费用户参赛用户付费金额',
               fquit_unum            bigint           comment '取消报名人数',
               fquit_cnt             bigint           comment '取消报名人次',
               fz_quit_unum          bigint           comment '主动取消报名人数',
               fz_quit_cnt           bigint           comment '主动取消报名人次',
               fb_quit_unum          bigint           comment '被动取消报名人数',
               fb_quit_cnt           bigint           comment '被动取消报名人次',
               fout_unum             bigint           comment '退赛人数',
               fout_cnt              bigint           comment '退赛人次',
               fz_out_unum           bigint           comment '主动退赛人数',
               fz_out_cnt            bigint           comment '主动退赛人次',
               fb_out_unum           bigint           comment '被动退赛人数',
               fb_out_cnt            bigint           comment '被动退赛人次',
               fdieout_unum          bigint           comment '淘汰人数',
               fdieout_cnt           bigint           comment '淘汰人次',
               fout_cost             decimal(20,2)    comment '发放成本',
               fin_cost              decimal(20,2)    comment '消耗成本',
               fjoin_suss_cnt        bigint           comment '报名开赛成功次数',
               faward_unum           bigint           comment '进入奖圈人数',
               faward_cnt            bigint           comment '进入奖圈次数',
               fcharge               bigint           comment '台费',
               fvictory_unum         bigint           comment '进入决胜圈人数',
               fvictory_num          bigint           comment '进入决胜圈次数',
               fgold_out             bigint           comment '金条产出',
               fgold_in              bigint           comment '金条回收',
               fbill_out             bigint           comment '发放话费',
               fparty_num            bigint           comment '牌局数',
               frelieve_unum         bigint           comment '复活人数',
               frelieve_num          bigint           comment '复活人次',
               ffrist_unum           bigint           comment '冠军人数',
               ffrist_cnt            bigint           comment '冠军次数'
               )comment '赛事明细'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_gsubgame_detail';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fparty_type', 'fpname', 'fsubname', 'fgsubname'],
                        'groups': [[1, 1, 1, 1],
                                   [1, 0, 0, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取报名相关数据
        hql = """--
            drop table if exists work.bud_user_match_gsubgame_detail_tmp_1_%(statdatenum)s;
          create table work.bud_user_match_gsubgame_detail_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,fitem_id          --报名物品id
                 ,fentry_fee        --报名费
                 ,fmatch_id         --比赛id
                 ,ffirst            --首次报名某一级比赛场
                 ,ffirst_sub        --首次报名某二级比赛场
                 ,ffirst_match      --首次报名任意比赛场
                 ,ffirst_gsub       --首次报名某三级赛事
                 ,fmatch_cfg_id     --比赛配置id
                 ,fpname            --牌局一级分类
                 ,fsubname          --牌局二级分类
                 ,fgsubname         --牌局三级分类
                 ,fparty_type       --牌局类型
                 ,faward_type       --奖励类型
                 ,fmatch_rule_type  --赛制类型
                 ,fmatch_rule_id    --赛制类型id
                 ,case when t2.fuid is not null then 1 else 0 end is_reg --是否当天注册用户
                 ,case when t3.fuid is not null then 1 else 0 end is_pay --是否当天付费用户
            from dim.join_gameparty t1
            left join dim.reg_user_main_additional t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            left join (select distinct fbpid,fuid
                         from dim.user_pay_day t3
                        where t3.dt = '%(statdate)s'
                      ) t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                 ,count(distinct fuid) fapply_unum         --报名人数
                 ,count(fuid) fapply_cnt                   --报名人次
                 ,count(distinct case when ffirst_match = 1 then fuid end) ffirst_apply_unum   --首次报名人数
                 ,count(distinct case when is_reg = 1 then fuid end) fnew_apply_unum           --新增用户报名人数
                 ,count(distinct case when fentry_fee = 0 then fuid end) fapply_fr_unum        --无偿报名人数
                 ,count(distinct case when fentry_fee > 0 then fuid end) fapply_co_unum        --有偿报名人数
                 ,0 fmatch_unum                           --参赛人数
                 ,0 fmatch_cnt                            --参赛人次
                 ,0 ffirst_match_unum                     --首次参赛人数
                 ,0 fnew_match_unum                       --新增用户参赛人数
                 ,0 fmatch_fr_unum                        --无偿参赛人数
                 ,0 fmatch_co_unum                        --有偿参赛人数
                 ,0 fwin_unum                             --获奖人数
                 ,0 fwin_num                              --获奖人次
                 ,0 fbegin_cnt                            --开赛次数
                 ,0 fbegin_suss_cnt                       --开赛成功次数
                 ,0 fbegin_fail_cnt                       --开赛失败次数
                 ,count(distinct case when is_pay = 1 and ffirst_match = 1 then fuid end) ffapply_pay_unum        --当日付费用户首次报名人数
                 ,0 ffmatch_pay_unum       --当日付费用户首次参赛人数
                 ,count(distinct case when is_pay = 1 then fuid end) fapply_pay_unum           --当日付费用户报名人数
                 ,count(case when is_pay = 1 then fuid end) fapply_pay_cnt          --当日付费用户报名次数
                 ,0 fapply_pay_income                                           --当日付费用户报名用户付费金额
                 ,0 fmatch_pay_unum                                                    --当日付费用户参赛人数
                 ,0 fmatch_pay_cnt        --当日付费用户参赛次数
                 ,0 fmatch_pay_income     --当日付费用户参赛用户付费金额
                 ,0 fquit_unum             --取消报名人数
                 ,0 fquit_cnt             --取消报名人次
                 ,0 fz_quit_unum          --主动取消报名人数
                 ,0 fz_quit_cnt           --主动取消报名人次
                 ,0 fb_quit_unum          --被动取消报名人数
                 ,0 fb_quit_cnt           --被动取消报名人次
                 ,0 fout_unum             --退赛人数
                 ,0 fout_cnt              --退赛人次
                 ,0 fz_out_unum           --主动退赛人数
                 ,0 fz_out_cnt            --主动退赛人次
                 ,0 fb_out_unum           --被动退赛人数
                 ,0 fb_out_cnt            --被动退赛人次
                 ,0 fdieout_unum          --淘汰人数
                 ,0 fdieout_cnt           --淘汰人次
                 ,cast (0 as decimal(20,2)) fout_cost             --发放成本
                 ,cast (0 as decimal(20,2)) fin_cost              --消耗成本
                 ,0 fjoin_suss_cnt        --报名开赛成功次数
                 ,0 faward_unum           --进奖圈人数
                 ,0 faward_cnt            --进奖圈次数
                 ,0 fcharge               --台费
                 ,0 fvictory_unum         --进入决胜圈人数
                 ,0 fvictory_num          --进入决胜圈次数
                 ,0 fgold_out             --金条产出
                 ,0 fgold_in              --金条回收
                 ,0 fbill_out             --发放话费
                 ,0 fparty_num            --牌局数
                 ,0 frelieve_unum         --复活人数
                 ,0 frelieve_num          --复活人次
                 ,0 ffrist_unum           --冠军人数
                 ,0 ffrist_cnt            --冠军次数
            from work.bud_user_match_gsubgame_detail_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type,fpname,fsubname,fgsubname
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """
            drop table if exists work.bud_user_match_gsubgame_detail_tmp_%(statdatenum)s;
          create table work.bud_user_match_gsubgame_detail_tmp_%(statdatenum)s as
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取参赛相关数据
        hql = """--
            drop table if exists work.bud_user_match_gsubgame_detail_tmp_2_%(statdatenum)s;
          create table work.bud_user_match_gsubgame_detail_tmp_2_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,fmatch_id         --比赛id
                 ,ffirst_play       --是否首次在该一级场次玩牌
                 ,ffirst_play_sub   --是否首次在该二级场次玩牌
                 ,ffirst_match      --首次参加比赛场
                 ,ffirst_play_gsub  --是否首次在该三级场次玩牌
                 ,fmatch_cfg_id     --比赛配置id
                 ,fcharge           --台费
                 ,ftbl_id
                 ,finning_id
                 ,fpname            --牌局一级分类
                 ,fsubname          --牌局二级分类
                 ,fgsubname         --牌局三级分类
                 ,fparty_type       --牌局类型
                 ,faward_type       --奖励类型
                 ,fmatch_rule_type  --赛制类型
                 ,fmatch_rule_id    --赛制类型id
                 ,fmode             --是否有偿报名参赛
                 ,case when t2.fuid is not null then 1 else 0 end is_reg --是否当天注册用户
                 ,case when t3.fuid is not null then 1 else 0 end is_pay --是否当天付费用户
            from dim.match_gameparty t1
            left join dim.reg_user_main_additional t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            left join (select distinct fbpid,fuid
                         from dim.user_pay_day t3
                        where t3.dt = '%(statdate)s'
                      ) t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                 ,0 fapply_unum         --报名人数
                 ,0 fapply_cnt                   --报名人次
                 ,0 ffirst_apply_unum   --首次报名人数
                 ,0 fnew_apply_unum           --新增用户报名人数
                 ,0 fapply_fr_unum        --无偿报名人数
                 ,0 fapply_co_unum        --有偿报名人数
                 ,count(distinct fuid) fmatch_unum        --参赛人数
                 ,count(distinct concat_ws('0', fmatch_id, cast (fuid as string))) fmatch_cnt    --参赛人次
                 ,count(distinct case when ffirst_match = 1 then fuid end) ffirst_match_unum  --首次参赛人数
                 ,count(distinct case when ffirst_match = 1 and is_reg = 1 then fuid end) fnew_match_unum    --新增用户参赛人数
                 ,count(distinct case when ffirst_match = 1 and fmode = 0 then fuid end) fmatch_fr_unum     --无偿参赛人数
                 ,count(distinct case when ffirst_match = 1 and fmode = 1 then fuid end) fmatch_co_unum     --有偿参赛人数
                 ,0 fwin_unum          --获奖人数
                 ,0 fwin_num           --获奖人次
                 ,0 fbegin_cnt         --开赛次数
                 ,0 fbegin_suss_cnt    --开赛成功次数
                 ,0 fbegin_fail_cnt    --开赛失败次数
                 ,0 ffapply_pay_unum        --当日付费用户首次报名人数
                 ,count(distinct case when is_pay = 1 and ffirst_match = 1 then fuid end) ffmatch_pay_unum       --当日付费用户首次参赛人数
                 ,0 fapply_pay_unum         --当日付费用户报名人数
                 ,0 fapply_pay_cnt          --当日付费用户报名次数
                 ,0 fapply_pay_income       --当日付费用户报名用户付费金额
                 ,count(distinct case when is_pay = 1 then fuid end) fmatch_pay_unum        --当日付费用户参赛人数
                 ,count(distinct case when is_pay = 1 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fmatch_pay_cnt        --当日付费用户参赛次数
                 ,0 fmatch_pay_income     --当日付费用户参赛用户付费金额
                 ,0 fquit_unum             --取消报名人数
                 ,0 fquit_cnt             --取消报名人次
                 ,0 fz_quit_unum          --主动取消报名人数
                 ,0 fz_quit_cnt           --主动取消报名人次
                 ,0 fb_quit_unum          --被动取消报名人数
                 ,0 fb_quit_cnt           --被动取消报名人次
                 ,0 fout_unum             --退赛人数
                 ,0 fout_cnt              --退赛人次
                 ,0 fz_out_unum           --主动退赛人数
                 ,0 fz_out_cnt            --主动退赛人次
                 ,0 fb_out_unum           --被动退赛人数
                 ,0 fb_out_cnt            --被动退赛人次
                 ,0 fdieout_unum          --淘汰人数
                 ,0 fdieout_cnt           --淘汰人次
                 ,0 fout_cost             --发放成本
                 ,0 fin_cost              --消耗成本
                 ,0 fjoin_suss_cnt        --报名开赛成功次数
                 ,0 faward_unum           --进奖圈人数
                 ,0 faward_cnt            --进奖圈次数
                 ,0 fcharge               --台费
                 ,0 fvictory_unum         --进入决胜圈人数
                 ,0 fvictory_num          --进入决胜圈次数
                 ,0 fgold_out             --金条产出
                 ,0 fgold_in              --金条回收
                 ,0 fbill_out             --发放话费
                 ,count(distinct concat_ws('0', ftbl_id, finning_id)) fparty_num            --牌局数
                 ,0 frelieve_unum         --复活人数
                 ,0 frelieve_num          --复活人次
                 ,0 ffrist_unum           --冠军人数
                 ,0 ffrist_cnt            --冠军次数
            from work.bud_user_match_gsubgame_detail_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type,fpname,fsubname,fgsubname
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """
            insert into table work.bud_user_match_gsubgame_detail_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取参赛相关数据
        hql = """--
            drop table if exists work.bud_user_match_gsubgame_detail_tmp_3_%(statdatenum)s;
          create table work.bud_user_match_gsubgame_detail_tmp_3_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) fmatch_id
                 ,t1.fuid
                 ,coalesce(t2.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t2.fgsubname,'%(null_str_report)s')  fgsubname
                 ,coalesce(t2.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t2.fpname,'%(null_str_report)s')  fpname
                 ,coalesce(t1.fact_id,'%(null_str_report)s')  fact_id     --途径id，区别发放消耗途径（1报名，2奖励，3轮间奖励，4取消报名，5复活
                 ,t1.fio_type          --发放消耗
                 ,t1.fitem_num         --物品数量
                 ,t1.fitem_id          --物品id
                 ,t1.fcost             --物品成本RMB
                 ,t1.frank
                 ,t2.fwin_num
                 ,t2.fvictory_num
            from stage.match_economy_stg t1
            left join dim.match_config t2
              on concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) = t2.fmatch_id
             and t2.dt = '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                 ,0 fapply_unum        --报名人数
                 ,0 fapply_cnt         --报名人次
                 ,0 ffirst_apply_unum  --首次报名人数
                 ,0 fnew_apply_unum    --新增用户报名人数
                 ,0 fapply_fr_unum     --无偿报名人数
                 ,0 fapply_co_unum     --有偿报名人数
                 ,0 fmatch_unum        --参赛人数
                 ,0 fmatch_cnt         --参赛人次
                 ,0 ffirst_match_unum  --首次参赛人数
                 ,0 fnew_match_unum    --新增用户参赛人数
                 ,0 fmatch_fr_unum     --无偿参赛人数
                 ,0 fmatch_co_unum     --有偿参赛人数
                 ,count(distinct case when fact_id = 2 then fuid end) fwin_unum          --获奖人数
                 ,count(case when fact_id = 2 then fuid end) fwin_num           --获奖人次
                 ,0 fbegin_cnt         --开赛次数
                 ,0 fbegin_suss_cnt    --开赛成功次数
                 ,0 fbegin_fail_cnt    --开赛失败次数
                 ,0 ffapply_pay_unum        --当日付费用户首次报名人数
                 ,0 ffmatch_pay_unum        --当日付费用户首次参赛人数
                 ,0 fapply_pay_unum         --当日付费用户报名人数
                 ,0 fapply_pay_cnt          --当日付费用户报名次数
                 ,0 fapply_pay_income       --当日付费用户报名用户付费金额
                 ,0 fmatch_pay_unum        --当日付费用户参赛人数
                 ,0 fmatch_pay_cnt        --当日付费用户参赛次数
                 ,0 fmatch_pay_income     --当日付费用户参赛用户付费金额
                 ,0 fquit_unum             --取消报名人数
                 ,0 fquit_cnt             --取消报名人次
                 ,0 fz_quit_unum          --主动取消报名人数
                 ,0 fz_quit_cnt           --主动取消报名人次
                 ,0 fb_quit_unum          --被动取消报名人数
                 ,0 fb_quit_cnt           --被动取消报名人次
                 ,0 fout_unum             --退赛人数
                 ,0 fout_cnt              --退赛人次
                 ,0 fz_out_unum           --主动退赛人数
                 ,0 fz_out_cnt            --主动退赛人次
                 ,0 fb_out_unum           --被动退赛人数
                 ,0 fb_out_cnt            --被动退赛人次
                 ,0 fdieout_unum          --淘汰人数
                 ,0 fdieout_cnt           --淘汰人次
                 ,sum(case when fio_type = 1 then fcost end) fout_cost             --发放成本
                 ,sum(case when fio_type = 2 then fcost end) fin_cost              --消耗成本
                 ,0 fjoin_suss_cnt        --报名开赛成功次数
                 ,count(distinct case when fact_id = '2' and frank > 0 and frank <= fwin_num then fuid end) faward_unum           --进奖圈人数
                 ,count(case when fact_id = '2' and frank > 0 and frank <= fwin_num then fuid end) faward_cnt            --进奖圈次数
                 ,sum(case when fact_id = 6 and fitem_id = '1' then fitem_num end) fcharge               --台费
                 ,count(distinct case when fact_id = '2' and frank > 0 and frank <= fvictory_num then fuid end) fvictory_unum         --进入决胜圈人数
                 ,count(case when fact_id = '2' and frank > 0 and frank <= fvictory_num then fuid end) fvictory_num          --进入决胜圈次数
                 ,sum(case when fio_type = 1 and fitem_id = '1' then fcost end) fgold_out             --金条产出
                 ,sum(case when fio_type = 2 and fitem_id = '1' then fcost end) fgold_in              --金条回收
                 ,sum(case when fio_type = 1 and fitem_id in ('200004','200005','200006','200007','200012','200013','200014','200015','400003') then fcost end) fbill_out             --发放话费
                 ,0 fparty_num            --牌局数
                 ,count(distinct case when fact_id = 5 and fitem_num > 0 then fuid end) frelieve_unum         --复活人数
                 ,count(case when fact_id = 5 and fitem_num > 0 then fuid end) frelieve_num          --复活人次
                 ,count(distinct case when fact_id = '2' and frank = 1 then fuid end) ffrist_unum           --冠军人数
                 ,count(distinct case when fact_id = '2' and frank = 1 then concat_ws('0', fmatch_id, cast (fuid as string)) end) ffrist_cnt            --冠军次数
            from work.bud_user_match_gsubgame_detail_tmp_3_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type,fpname,fsubname,fgsubname
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """
            insert into table work.bud_user_match_gsubgame_detail_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
            insert overwrite table bud_dm.bud_user_match_gsubgame_detail partition(dt='%(statdate)s')
        select '%(statdate)s' fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,t.fparty_type
               ,t.fpname
               ,t.fsubname
               ,t.fgsubname
               ,fapply_unum
               ,fapply_cnt
               ,ffirst_apply_unum
               ,fnew_apply_unum
               ,fapply_fr_unum
               ,fapply_co_unum
               ,fmatch_unum
               ,fmatch_cnt
               ,ffirst_match_unum
               ,fnew_match_unum
               ,fmatch_fr_unum
               ,fmatch_co_unum
               ,fwin_unum
               ,fwin_num
               ,nvl(t2.fbegin_suss_cnt,0) + nvl(t3.fbegin_fail_cnt,0) fbegin_cnt
               ,nvl(t2.fbegin_suss_cnt,0) fbegin_suss_cnt
               ,nvl(t3.fbegin_fail_cnt,0) fbegin_fail_cnt
               ,ffapply_pay_unum
               ,ffmatch_pay_unum
               ,fapply_pay_unum
               ,fapply_pay_cnt
               ,fapply_pay_income
               ,fmatch_pay_unum
               ,fmatch_pay_cnt
               ,fmatch_pay_income
               ,fquit_unum
               ,fquit_cnt
               ,fz_quit_unum
               ,fz_quit_cnt
               ,fb_quit_unum
               ,fb_quit_cnt
               ,fout_unum
               ,fout_cnt
               ,fz_out_unum
               ,fz_out_cnt
               ,fb_out_unum
               ,fb_out_cnt
               ,fdieout_unum
               ,fdieout_cnt
               ,fout_cost
               ,fin_cost
               ,t2.fjoin_suss_cnt
               ,faward_unum
               ,faward_cnt
               ,fcharge
               ,fvictory_unum
               ,fvictory_num
               ,fgold_out
               ,fgold_in
               ,fbill_out
               ,fparty_num
               ,frelieve_unum
               ,frelieve_num
               ,ffrist_unum
               ,ffrist_cnt
          from (select  fgamefsk
                       ,fplatformfsk
                       ,fhallfsk
                       ,fsubgamefsk
                       ,fterminaltypefsk
                       ,fversionfsk
                       ,t.fparty_type
                       ,t.fpname
                       ,t.fsubname
                       ,t.fgsubname
                       ,sum(fapply_unum) fapply_unum
                       ,sum(fapply_cnt) fapply_cnt
                       ,sum(ffirst_apply_unum) ffirst_apply_unum
                       ,sum(fnew_apply_unum) fnew_apply_unum
                       ,sum(fapply_fr_unum) fapply_fr_unum
                       ,sum(fapply_co_unum) fapply_co_unum
                       ,sum(fmatch_unum) fmatch_unum
                       ,sum(fmatch_cnt) fmatch_cnt
                       ,sum(ffirst_match_unum) ffirst_match_unum
                       ,sum(fnew_match_unum) fnew_match_unum
                       ,sum(fmatch_fr_unum) fmatch_fr_unum
                       ,sum(fmatch_co_unum) fmatch_co_unum
                       ,sum(fwin_unum) fwin_unum
                       ,sum(fwin_num) fwin_num
                       ,sum(fbegin_cnt) fbegin_cnt
                       ,sum(fbegin_suss_cnt) fbegin_suss_cnt
                       ,sum(fbegin_fail_cnt) fbegin_fail_cnt
                       ,sum(ffapply_pay_unum) ffapply_pay_unum
                       ,sum(ffmatch_pay_unum) ffmatch_pay_unum
                       ,sum(fapply_pay_unum) fapply_pay_unum
                       ,sum(fapply_pay_cnt) fapply_pay_cnt
                       ,sum(fapply_pay_income) fapply_pay_income
                       ,sum(fmatch_pay_unum) fmatch_pay_unum
                       ,sum(fmatch_pay_cnt) fmatch_pay_cnt
                       ,sum(fmatch_pay_income) fmatch_pay_income
                       ,sum(fquit_unum) fquit_unum
                       ,sum(fquit_cnt) fquit_cnt
                       ,sum(fz_quit_unum) fz_quit_unum
                       ,sum(fz_quit_cnt) fz_quit_cnt
                       ,sum(fb_quit_unum) fb_quit_unum
                       ,sum(fb_quit_cnt) fb_quit_cnt
                       ,sum(fout_unum) fout_unum
                       ,sum(fout_cnt) fout_cnt
                       ,sum(fz_out_unum) fz_out_unum
                       ,sum(fz_out_cnt) fz_out_cnt
                       ,sum(fb_out_unum) fb_out_unum
                       ,sum(fb_out_cnt) fb_out_cnt
                       ,sum(fdieout_unum) fdieout_unum
                       ,sum(fdieout_cnt) fdieout_cnt
                       ,sum(fout_cost) fout_cost
                       ,sum(fin_cost) fin_cost
                       ,sum(fjoin_suss_cnt) fjoin_suss_cnt
                       ,sum(faward_unum) faward_unum
                       ,sum(faward_cnt) faward_cnt
                       ,sum(fcharge) fcharge
                       ,sum(fvictory_unum) fvictory_unum
                       ,sum(fvictory_num) fvictory_num
                       ,sum(fgold_out) fgold_out
                       ,sum(fgold_in) fgold_in
                       ,sum(fbill_out) fbill_out
                       ,sum(fparty_num) fparty_num
                       ,sum(frelieve_unum) frelieve_unum
                       ,sum(frelieve_num) frelieve_num
                       ,sum(ffrist_unum) ffrist_unum
                       ,sum(ffrist_cnt) ffrist_cnt
                  from (select  fgamefsk
                               ,fplatformfsk
                               ,fhallfsk
                               ,fgame_id fsubgamefsk
                               ,fterminaltypefsk
                               ,fversionfsk
                               ,fparty_type
                               ,fpname
                               ,fsubname
                               ,fgsubname
                               ,fapply_unum
                               ,fapply_cnt
                               ,ffirst_apply_unum
                               ,fnew_apply_unum
                               ,fapply_fr_unum
                               ,fapply_co_unum
                               ,fmatch_unum
                               ,fmatch_cnt
                               ,ffirst_match_unum
                               ,fnew_match_unum
                               ,fmatch_fr_unum
                               ,fmatch_co_unum
                               ,fwin_unum
                               ,fwin_num
                               ,fbegin_cnt
                               ,fbegin_suss_cnt
                               ,fbegin_fail_cnt
                               ,ffapply_pay_unum
                               ,ffmatch_pay_unum
                               ,fapply_pay_unum
                               ,fapply_pay_cnt
                               ,fapply_pay_income
                               ,fmatch_pay_unum
                               ,fmatch_pay_cnt
                               ,fmatch_pay_income
                               ,fquit_unum
                               ,fquit_cnt
                               ,fz_quit_unum
                               ,fz_quit_cnt
                               ,fb_quit_unum
                               ,fb_quit_cnt
                               ,fout_unum
                               ,fout_cnt
                               ,fz_out_unum
                               ,fz_out_cnt
                               ,fb_out_unum
                               ,fb_out_cnt
                               ,fdieout_unum
                               ,fdieout_cnt
                               ,fout_cost
                               ,fin_cost
                               ,fjoin_suss_cnt
                               ,faward_unum
                               ,faward_cnt
                               ,fcharge
                               ,fvictory_unum
                               ,fvictory_num
                               ,fgold_out
                               ,fgold_in
                               ,fbill_out
                               ,fparty_num
                               ,frelieve_unum
                               ,frelieve_num
                               ,ffrist_unum
                               ,ffrist_cnt
                          from work.bud_user_match_gsubgame_detail_tmp_%(statdatenum)s
                         union all
                        select  fgamefsk
                               ,fplatformfsk
                               ,fhallfsk
                               ,fsubgamefsk
                               ,fterminaltypefsk
                               ,fversionfsk
                               ,fparty_type
                               ,fpname
                               ,fsubname
                               ,fgsubname
                               ,0 fapply_unum
                               ,0 fapply_cnt
                               ,0 ffirst_apply_unum
                               ,0 fnew_apply_unum
                               ,0 fapply_fr_unum
                               ,0 fapply_co_unum
                               ,0 fmatch_unum
                               ,0 fmatch_cnt
                               ,0 ffirst_match_unum
                               ,0 fnew_match_unum
                               ,0 fmatch_fr_unum
                               ,0 fmatch_co_unum
                               ,0 fwin_unum
                               ,0 fwin_num
                               ,0 fbegin_cnt
                               ,0 fbegin_suss_cnt
                               ,0 fbegin_fail_cnt
                               ,0 ffapply_pay_unum
                               ,0 ffmatch_pay_unum
                               ,0 fapply_pay_unum
                               ,0 fapply_pay_cnt
                               ,0 fapply_pay_income
                               ,0 fmatch_pay_unum
                               ,0 fmatch_pay_cnt
                               ,0 fmatch_pay_income
                               ,fquit_unum
                               ,fquit_cnt
                               ,fz_quit_unum
                               ,fz_quit_cnt
                               ,fb_quit_unum
                               ,fb_quit_cnt
                               ,fout_unum
                               ,fout_cnt
                               ,fz_out_unum
                               ,fz_out_cnt
                               ,fb_out_unum
                               ,fb_out_cnt
                               ,fdieout_unum
                               ,fdieout_cnt
                               ,0 fout_cost
                               ,0 fin_cost
                               ,0 fjoin_suss_cnt
                               ,0 faward_unum
                               ,0 faward_cnt
                               ,0 fcharge
                               ,0 fvictory_unum
                               ,0 fvictory_num
                               ,0 fgold_out
                               ,0 fgold_in
                               ,0 fbill_out
                               ,0 fparty_num
                               ,0 frelieve_unum
                               ,0 frelieve_num
                               ,0 ffrist_unum
                               ,0 ffrist_cnt
                          from bud_dm.bud_user_quit_match_detail t
                         where t.dt = '%(statdate)s'
                           and t.fparty_type <> '-21379'
                           and t.fpname <> '-21379'
                           and t.fsubname <> '-21379'
                           and t.fgsubname <> '-21379'
                           and t.fmatch_id = '-21379'
                           and t.fround_num = '-21379'
                           and t.fgame_num = '-21379'
                           ) t
                 group by fgamefsk
                         ,fplatformfsk
                         ,fhallfsk
                         ,fsubgamefsk
                         ,fterminaltypefsk
                         ,fversionfsk
                         ,t.fparty_type
                         ,t.fpname
                         ,t.fsubname
                         ,t.fgsubname) t
                  left join (select coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                                    ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                                    ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                                    ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                                    ,count(distinct case when fis_fail = 0 then fmatch_id end) fbegin_suss_cnt
                                    ,count(distinct case when fjoin_unum > 0 and fparty_unum > 0 then fmatch_id end) fjoin_suss_cnt
                               from dim.match_config t
                              where dt = '%(statdate)s' and fis_fail = 0
                              group by fparty_type
                                       ,fpname
                                       ,fsubname
                                       ,fgsubname
                           grouping sets ( (fparty_type),
                                           (fparty_type ,fpname ,fsubname ,fgsubname)
                                         )
                            ) t2
                    on t.fparty_type = t2.fparty_type
                   and t.fpname = t2.fpname
                   and t.fsubname = t2.fsubname
                   and t.fgsubname = t2.fgsubname
                  left join (select coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                                    ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                                    ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                                    ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                                    ,count(distinct case when fis_fail = 1 then fmatch_id end) fbegin_fail_cnt
                               from stage.match_config_stg t
                              where dt = '%(statdate)s' and fis_fail = 1
                              group by fparty_type
                                       ,fpname
                                       ,fsubname
                                       ,fgsubname
                           grouping sets ( (fparty_type),
                                           (fparty_type ,fpname ,fsubname ,fgsubname)
                                         )
                            ) t3
                    on t.fparty_type = t3.fparty_type
                   and t.fpname = t3.fpname
                   and t.fsubname = t3.fsubname
                   and t.fgsubname = t3.fgsubname
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_gsubgame_detail_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_match_gsubgame_detail_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_match_gsubgame_detail_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_user_match_gsubgame_detail_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_gsubgame_detail(sys.argv[1:])
a()
