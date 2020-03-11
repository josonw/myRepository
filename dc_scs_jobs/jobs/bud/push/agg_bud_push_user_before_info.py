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

class agg_bud_push_user_before_info(BaseStatModel):
    def create_tab(self):

        hql = """--推送效果分析-推送前及推送结果数据
        create table if not exists bud_dm.bud_push_user_before_info
              (fdate              date,
               fpush_id           bigint comment '推送id',
               fpush_num          bigint comment '总推送用户量',
               fappear_num        bigint comment '推送展示量',
               fclick_num         bigint comment '推送点击量',
               fsuccess_num       bigint comment '推送送达量',
               fplay_unum_all     bigint comment '推送前7日玩牌人数（总量）',
               fact_unum_all      bigint comment '推送前7日活跃人数（总量）',
               fpay_unum_all      bigint comment '推送付费人数（总量）',
               fact_unum_before_1 bigint comment '推送前1日活跃人数',
               fact_unum_before_2 bigint comment '推送前2日活跃人数',
               fact_unum_before_3 bigint comment '推送前3日活跃人数',
               fact_unum_before_4 bigint comment '推送前4日活跃人数',
               fact_unum_before_5 bigint comment '推送前5日活跃人数',
               fact_unum_before_6 bigint comment '推送前6日活跃人数',
               fact_unum_before_7 bigint comment '推送前7日活跃人数'
              )comment '推送效果分析-推送前及推送结果数据'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_push_user_before_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.bud_push_user_before_info_tmp_1_%(statdatenum)s;
        create table work.bud_push_user_before_info_tmp_1_%(statdatenum)s as
            select distinct t2.fbpid, t1.fuid, t1.fpush_id
              from dim.user_push_report t1
              join stage.push_config_stg t2
                on t1.fpush_id = t2.fpush_id
               and substr(t2.fbegin_time, 1, 10) = '%(ld_1day_ago)s'
               and t2.dt <= '%(statdate)s'
               and t2.ftoken_id = 0
               and t2.fstatus = 3
             where t1.faction = 1
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.bud_push_user_before_info_tmp_2_%(statdatenum)s;
        create table work.bud_push_user_before_info_tmp_2_%(statdatenum)s as
        select fpush_id
               ,sum(case when dt = date_sub(cast('%(ld_1day_ago)s' as date),1) then act_user end) fact_unum_before_1
               ,sum(case when dt = date_sub(cast('%(ld_1day_ago)s' as date),2) then act_user end) fact_unum_before_2
               ,sum(case when dt = date_sub(cast('%(ld_1day_ago)s' as date),3) then act_user end) fact_unum_before_3
               ,sum(case when dt = date_sub(cast('%(ld_1day_ago)s' as date),4) then act_user end) fact_unum_before_4
               ,sum(case when dt = date_sub(cast('%(ld_1day_ago)s' as date),5) then act_user end) fact_unum_before_5
               ,sum(case when dt = date_sub(cast('%(ld_1day_ago)s' as date),6) then act_user end) fact_unum_before_6
               ,sum(case when dt = date_sub(cast('%(ld_1day_ago)s' as date),7) then act_user end) fact_unum_before_7
          from (select t2.dt, t1.fpush_id,count(distinct t1.fuid) act_user
                  from work.bud_push_user_before_info_tmp_1_%(statdatenum)s t1
                  join dim.user_act t2
                    on t1.fbpid = t2.fbpid
                   and t1.fuid = t2.fuid
                   and t2.dt >= date_sub(cast('%(ld_1day_ago)s' as date),7)
                   and t2.dt <= date_sub(cast('%(ld_1day_ago)s' as date),1)
                 group by t2.dt, t1.fpush_id
               ) t
         group by fpush_id

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.bud_push_user_before_info_tmp_3_%(statdatenum)s;
        create table work.bud_push_user_before_info_tmp_3_%(statdatenum)s as
            select t1.fpush_id,count(distinct t1.fuid) act_user,count(distinct case when fparty_num > 0 then t1.fuid end) play_user
              from work.bud_push_user_before_info_tmp_1_%(statdatenum)s t1
              join dim.user_act t2
                on t1.fbpid = t2.fbpid
               and t1.fuid = t2.fuid
                   and t2.dt >= date_sub(cast('%(ld_1day_ago)s' as date),7)
                   and t2.dt <= '%(ld_1day_ago)s'
             group by t1.fpush_id
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.bud_push_user_before_info_tmp_4_%(statdatenum)s;
        create table work.bud_push_user_before_info_tmp_4_%(statdatenum)s as
            select t1.fpush_id,count(distinct t1.fuid) pay_user
              from work.bud_push_user_before_info_tmp_1_%(statdatenum)s t1
              join dim.user_pay t2
                on t1.fbpid = t2.fbpid
               and t1.fuid = t2.fuid
             group by t1.fpush_id
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table bud_dm.bud_push_user_before_info  partition( dt="%(ld_1day_ago)s" )
            select '%(ld_1day_ago)s' fdate
                   ,t1.fpush_id
                   ,t1.fpush_num fpush_num
                   ,t1.fappear_num fappear_num
                   ,t1.fclick_num fclick_num
                   ,t1.fsuccess_num fsuccess_num
                   ,coalesce(t3.play_user,0) fplay_unum_all
                   ,coalesce(t3.act_user,0) fact_unum_all
                   ,coalesce(t4.pay_user,0) fpay_unum_all
                   ,coalesce(t2.fact_unum_before_1,0) fact_unum_before_1
                   ,coalesce(t2.fact_unum_before_2,0) fact_unum_before_2
                   ,coalesce(t2.fact_unum_before_3,0) fact_unum_before_3
                   ,coalesce(t2.fact_unum_before_4,0) fact_unum_before_4
                   ,coalesce(t2.fact_unum_before_5,0) fact_unum_before_5
                   ,coalesce(t2.fact_unum_before_6,0) fact_unum_before_6
                   ,coalesce(t2.fact_unum_before_7,0) fact_unum_before_7
              from stage.push_config_stg t1
              left join work.bud_push_user_before_info_tmp_2_%(statdatenum)s t2
                on t1.fpush_id = t2.fpush_id
              left join work.bud_push_user_before_info_tmp_3_%(statdatenum)s t3
                on t1.fpush_id = t3.fpush_id
              left join work.bud_push_user_before_info_tmp_4_%(statdatenum)s t4
                on t1.fpush_id = t4.fpush_id
             where substr(t1.fbegin_time, 1, 10) = '%(ld_1day_ago)s'
               and t1.dt <= '%(ld_1day_ago)s'
               and t1.ftoken_id = 0
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_push_user_before_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_push_user_before_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_push_user_before_info_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_push_user_before_info_tmp_4_%(statdatenum)s;

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = agg_bud_push_user_before_info(sys.argv[1:])
a()
