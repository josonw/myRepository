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


class agg_bud_user_match_pay_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_pay_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               ffapply_pay_unum      bigint          comment '当日付费用户首次报名人数',
               ffmatch_pay_unum      bigint          comment '当日付费用户首次参赛人数',
               ffapply_paid_unum     bigint          comment '历史付费用户首次报名人数',
               ffmatch_paid_unum     bigint          comment '历史付费用户首次参赛人数',
               fapply_pay_unum       bigint          comment '当日付费用户报名人数',
               fapply_pay_cnt        bigint          comment '当日付费用户报名次数',
               fapply_pay_income     decimal(20,2)   comment '当日付费用户报名用户付费金额',
               fmatch_pay_unum       bigint          comment '当日付费用户参赛人数',
               fmatch_pay_cnt        bigint          comment '当日付费用户参赛次数',
               fmatch_pay_income     decimal(20,2)   comment '当日付费用户参赛用户付费金额',
               fapply_paid_unum      bigint          comment '历史付费用户报名人数',
               fapply_paid_cnt       bigint          comment '历史付费用户报名次数',
               fmatch_paid_unum      bigint          comment '历史付费用户参赛人数',
               fmatch_paid_cnt       bigint          comment '历史付费用户参赛次数',
               fmatch_paycnt         bigint          comment '当日付费参赛用户付费次数',
               fapply_paycnt         bigint          comment '当日付费报名用户付费次数'
               )comment '用户赛事付费相关信息'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_pay_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """ --
            drop table if exists work.bud_user_match_pay_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_match_pay_info_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t4.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,t1.ffirst_apply      --是否首次报名
                 ,t1.fnew_apply        --是否注册用户报名
                 ,t1.join_num
                 ,case when t2.fuid is not null then 1 else 0 end is_paid
                 ,case when t3.fuid is not null then 1 else 0 end is_pay
                 ,fcoins_num*frate ftotal_usd_amt
                 ,t3.forder_id
            from (select jg.fbpid
                         ,jg.fuid
                         ,max(ffirst_match) ffirst_apply
                         ,max(case when ru.fuid is not null then 1 else 0 end) fnew_apply
                         ,count(1) join_num
                    from stage.join_gameparty_stg jg
                    left join dim.reg_user_main_additional ru
                      on jg.fbpid = ru.fbpid
                     and jg.fuid = ru.fuid
                     and ru.dt = '%(statdate)s'
                   where jg.dt = '%(statdate)s'
                     and (coalesce(fmatch_cfg_id,'0')<>'0' or coalesce(fmatch_log_id,'0')<>'0')
                   group by jg.fbpid,jg.fuid
                 ) t1
            left join dim.user_pay t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt < '%(statdate)s'
            left join stage.payment_stream_stg t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            left join stage.user_generate_order_stg t4
              on t3.forder_id = t4.forder_id
             and t4.dt = '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
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
                 ,count(distinct case when is_pay = 1 and ffirst_apply = 1 then fuid end) ffapply_pay_unum          --当日付费用户首次报名人数
                 ,0 ffmatch_pay_unum          --当日付费用户首次参赛人数
                 ,count(distinct case when is_paid = 1 and ffirst_apply = 1 then fuid end) ffapply_paid_unum        --历史付费用户首次报名人数
                 ,0 ffmatch_paid_unum        --历史付费用户首次参赛人数
                 ,count(distinct case when is_pay = 1 then fuid end) fapply_pay_unum             --当日付费用户报名人数
                 ,sum(join_num) fapply_pay_cnt          --当日付费用户报名次数
                 ,sum(case when is_pay = 1 then ftotal_usd_amt end) fapply_pay_income            --当日付费用户报名用户付费金额
                 ,0 fmatch_pay_unum            --当日付费用户参赛人数
                 ,0 fmatch_pay_cnt        --当日付费用户参赛次数
                 ,cast (0 as decimal(20,2)) fmatch_pay_income          --当日付费用户参赛用户付费金额
                 ,count(distinct case when is_paid = 1 then fuid end) fapply_paid_unum           --历史付费用户报名人数
                 ,sum(case when is_paid = 1 then join_num end) fapply_paid_cnt        --历史付费用户报名次数
                 ,0 fmatch_paid_unum          --历史付费用户参赛人数
                 ,0 fmatch_paid_cnt       --历史付费用户参赛次数
                 ,0 fmatch_paycnt   --当日付费参赛用户付费次数
                 ,count(distinct case when is_pay = 1 then forder_id end) fapply_paycnt    --当日付费报名用户付费次数
            from work.bud_user_match_pay_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """
                drop table if exists work.bud_user_match_pay_info_tmp_%(statdatenum)s;
              create table work.bud_user_match_pay_info_tmp_%(statdatenum)s as
                      %(sql_template)s;"""

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_match_pay_info_tmp_2_%(statdatenum)s;
          create table work.bud_user_match_pay_info_tmp_2_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t4.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,t1.ffirst_party      --是否首次参赛
                 ,t1.fnew_party        --是否注册用户参赛
                 ,t1.party_num
                 ,case when t2.fuid is not null then 1 else 0 end is_paid
                 ,case when t3.fuid is not null then 1 else 0 end is_pay
                 ,fcoins_num*frate ftotal_usd_amt
                 ,t3.forder_id
            from (select fbpid
                         ,fuid
                         ,max(ffirst_party) ffirst_party
                         ,max(fnew_party) fnew_party
                         ,count(distinct concat_ws('0', fmatch_id, cast (fuid as string))) party_num
                    from dim.match_user
                   where dt = '%(statdate)s'
                     and fparty_flag = 1
                   group by fbpid,fuid
                 ) t1
            left join dim.user_pay t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt < '%(statdate)s'
            left join stage.payment_stream_stg t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            left join stage.user_generate_order_stg t4
              on t3.forder_id = t4.forder_id
             and t4.dt = '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
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
                 ,0 ffapply_pay_unum          --当日付费用户首次报名人数
                 ,count(distinct case when is_pay = 1 and ffirst_party = 1 then fuid end) ffmatch_pay_unum          --当日付费用户首次参赛人数
                 ,0 ffapply_paid_unum        --历史付费用户首次报名人数
                 ,count(distinct case when is_paid = 1 and ffirst_party = 1 then fuid end) ffmatch_paid_unum          --历史付费用户首次参赛人数
                 ,0 fapply_pay_unum            --当日付费用户报名人数
                 ,0 fapply_pay_cnt             --当日付费用户报名次数
                 ,0 fapply_pay_income          --当日付费用户报名用户付费金额
                 ,count(distinct case when is_pay = 1 then fuid end) fmatch_pay_unum            --当日付费用户参赛人数
                 ,sum(party_num) fmatch_pay_cnt        --当日付费用户参赛次数
                 ,sum(case when is_pay = 1 then ftotal_usd_amt end) fmatch_pay_income          --当日付费用户参赛用户付费金额
                 ,0 fapply_paid_unum      --历史付费用户报名人数
                 ,0 fapply_paid_cnt       --历史付费用户报名次数
                 ,count(distinct case when is_paid = 1 then fuid end) fmatch_paid_unum    --历史付费用户参赛人数
                 ,sum(case when is_paid = 1 then party_num end) fmatch_paid_cnt           --历史付费用户参赛次数
                 ,count(distinct case when is_pay = 1 then forder_id end) fmatch_paycnt   --当日付费参赛用户付费次数
                 ,0 fapply_paycnt    --当日付费报名用户付费次数
            from work.bud_user_match_pay_info_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """insert into table work.bud_user_match_pay_info_tmp_%(statdatenum)s
                      %(sql_template)s"""

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """insert overwrite table bud_dm.bud_user_match_pay_info
            partition(dt='%(statdate)s')
          select '%(statdate)s' fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,sum(ffapply_pay_unum) ffapply_pay_unum      --当日付费用户首次报名人数
                 ,sum(ffmatch_pay_unum) ffmatch_pay_unum      --当日付费用户首次参赛人数
                 ,sum(ffapply_paid_unum) ffapply_paid_unum    --历史付费用户首次报名人数
                 ,sum(ffmatch_paid_unum) ffmatch_paid_unum    --历史付费用户首次参赛人数
                 ,sum(fapply_pay_unum) fapply_pay_unum        --当日付费用户报名人数
                 ,sum(fapply_pay_cnt) fapply_pay_cnt          --当日付费用户报名次数
                 ,sum(fapply_pay_income) fapply_pay_income    --当日付费用户报名用户付费金额
                 ,sum(fmatch_pay_unum) fmatch_pay_unum        --当日付费用户参赛人数
                 ,sum(fmatch_pay_cnt) fmatch_pay_cnt          --当日付费用户参赛次数
                 ,sum(fmatch_pay_income) fmatch_pay_income    --当日付费用户参赛用户付费金额
                 ,sum(fapply_paid_unum) fapply_paid_unum      --历史付费用户报名人数
                 ,sum(fapply_paid_cnt) fapply_paid_cnt        --历史付费用户报名次数
                 ,sum(fmatch_paid_unum) fmatch_paid_unum      --历史付费用户参赛人数
                 ,sum(fmatch_paid_cnt) fmatch_paid_cnt        --历史付费用户参赛次数
                 ,sum(fmatch_paycnt) fmatch_paycnt            --当日付费参赛用户付费次数
                 ,sum(fapply_paycnt) fapply_paycnt            --当日付费报名用户付费次数
            from work.bud_user_match_pay_info_tmp_%(statdatenum)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_pay_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_match_pay_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_match_pay_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_pay_info(sys.argv[1:])
a()
