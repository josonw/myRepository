#! /usr/local/python272/bin/python
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

class agg_user_action_hour_dis(BaseStatModel):

    def create_tab(self):

        hql = """create table if not exists dcnew.user_action_hour_dis
            (
            fdate date comment '数据日期',
            fgamefsk bigint comment '游戏ID',
            fplatformfsk bigint comment '平台ID',
            fhallfsk bigint comment '大厅ID',
            fsubgamefsk bigint comment '子游戏ID',
            fterminaltypefsk bigint comment '终端类型ID',
            fversionfsk bigint comment '应用包版本ID',
            fchannelcode bigint comment '渠道ID',
            fdimension varchar(32) comment '指标类型，ds表示每日注册,dalgnucnt表示活跃登录人数，dalgncnt表示活跃登录次数,dagptucnt表示活跃玩牌人数',
            fh1 bigint comment '1点钟对应统计指标人数',
            fh2 bigint comment '2点钟对应统计指标人数',
            fh3 bigint comment '3点钟对应统计指标人数',
            fh4 bigint comment '4点钟对应统计指标人数',
            fh5 bigint comment '5点钟对应统计指标人数',
            fh6 bigint comment '6点钟对应统计指标人数',
            fh7 bigint comment '7点钟对应统计指标人数',
            fh8 bigint comment '8点钟对应统计指标人数',
            fh9 bigint comment '9点钟对应统计指标人数',
            fh10 bigint comment '10点钟对应统计指标人数',
            fh11 bigint comment '11点钟对应统计指标人数',
            fh12 bigint comment '12点钟对应统计指标人数',
            fh13 bigint comment '13点钟对应统计指标人数',
            fh14 bigint comment '14点钟对应统计指标人数',
            fh15 bigint comment '15点钟对应统计指标人数',
            fh16 bigint comment '16点钟对应统计指标人数',
            fh17 bigint comment '17点钟对应统计指标人数',
            fh18 bigint comment '18点钟对应统计指标人数',
            fh19 bigint comment '19点钟对应统计指标人数',
            fh20 bigint comment '20点钟对应统计指标人数',
            fh21 bigint comment '21点钟对应统计指标人数',
            fh22 bigint comment '22点钟对应统计指标人数',
            fh23 bigint comment '23点钟对应统计指标人数',
            fh24 bigint comment '24点钟对应统计指标人数'
            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


    def stat(self):

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.user_action_hour_dis
        partition(dt = '%(statdate)s' )
            select
            '%(statdate)s' fdate,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fgame_id fsubgamefsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fchannel_code fchannelcode,
            'ds' fdimension,
            coalesce(count(case when hour(un.fsignup_at) = 1-1 then un.fuid else null end),0) fh1,
            coalesce(count(case when hour(un.fsignup_at) = 2-1 then un.fuid else null end),0) fh2,
            coalesce(count(case when hour(un.fsignup_at) = 3-1 then un.fuid else null end),0) fh3,
            coalesce(count(case when hour(un.fsignup_at) = 4-1 then un.fuid else null end),0) fh4,
            coalesce(count(case when hour(un.fsignup_at) = 5-1 then un.fuid else null end),0) fh5,
            coalesce(count(case when hour(un.fsignup_at) = 6-1 then un.fuid else null end),0) fh6,
            coalesce(count(case when hour(un.fsignup_at) = 7-1 then un.fuid else null end),0) fh7,
            coalesce(count(case when hour(un.fsignup_at) = 8-1 then un.fuid else null end),0) fh8,
            coalesce(count(case when hour(un.fsignup_at) = 9-1 then un.fuid else null end),0) fh9,
            coalesce(count(case when hour(un.fsignup_at) = 10-1 then un.fuid else null end),0) fh10,
            coalesce(count(case when hour(un.fsignup_at) = 11-1 then un.fuid else null end),0) fh11,
            coalesce(count(case when hour(un.fsignup_at) = 12-1 then un.fuid else null end),0) fh12,
            coalesce(count(case when hour(un.fsignup_at) = 13-1 then un.fuid else null end),0) fh13,
            coalesce(count(case when hour(un.fsignup_at) = 14-1 then un.fuid else null end),0) fh14,
            coalesce(count(case when hour(un.fsignup_at) = 15-1 then un.fuid else null end),0) fh15,
            coalesce(count(case when hour(un.fsignup_at) = 16-1 then un.fuid else null end),0) fh16,
            coalesce(count(case when hour(un.fsignup_at) = 17-1 then un.fuid else null end),0) fh17,
            coalesce(count(case when hour(un.fsignup_at) = 18-1 then un.fuid else null end),0) fh18,
            coalesce(count(case when hour(un.fsignup_at) = 19-1 then un.fuid else null end),0) fh19,
            coalesce(count(case when hour(un.fsignup_at) = 20-1 then un.fuid else null end),0) fh20,
            coalesce(count(case when hour(un.fsignup_at) = 21-1 then un.fuid else null end),0) fh21,
            coalesce(count(case when hour(un.fsignup_at) = 22-1 then un.fuid else null end),0) fh22,
            coalesce(count(case when hour(un.fsignup_at) = 23-1 then un.fuid else null end),0) fh23,
            coalesce(count(case when hour(un.fsignup_at) = 24-1 then un.fuid else null end),0) fh24
        from
            dim.reg_user_array un
        where
            un.dt = '%(statdate)s'
        group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,
        un.fversionfsk,un.fchannel_code
        """

        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.user_action_hour_dis_login_%(statdatenum)s;
        create table work.user_action_hour_dis_login_%(statdatenum)s
        as
        select coalesce(fgamefsk,%(null_int_group_rule)d) fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
            coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
            fhour,
            count(1) flogincnt,
            count(distinct fuid) floginunum
        from (
            select  /*+ mapjoin(ci, bm) */
                    bm.fgamefsk,
                    bm.fplatformfsk,
                    bm.fhallfsk,
                    coalesce(uei.fgame_id,cast (0 as bigint)) fgame_id,
                    bm.fterminaltypefsk,
                    bm.fversionfsk,
                    coalesce(ci.ftrader_id,%(null_int_report)d) fchannel_code,
                    fuid,
                    hour(flts_at)+1 fhour
            from stage.user_enter_stg uei
            left join analysis.marketing_channel_pkg_info ci
              on uei.fchannel_code = ci.fid
            join dim.bpid_map bm
              on uei.fbpid = bm.fbpid
            where dt = '%(statdate)s'
        ) t
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fchannel_code, fhour
        grouping sets ((fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fhour),
                        (fgamefsk,fplatformfsk,fhallfsk, fgame_id, fhour),
                        (fgamefsk, fgame_id, fhour))

        union all

        select coalesce(fgamefsk,%(null_int_group_rule)d) fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
            coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
            fhour,
            count(1) flogincnt,
            count(distinct fuid) floginunum
        from (
            select  /*+ mapjoin(ci, bm) */
                    bm.fgamefsk,
                    bm.fplatformfsk,
                    bm.fhallfsk,
                    null fgame_id,
                    bm.fterminaltypefsk,
                    bm.fversionfsk,
                    coalesce(ci.ftrader_id,%(null_int_report)d) fchannel_code,
                    fuid,
                    hour(flogin_at)+1 fhour
            from dim.user_login_additional uei
            left join analysis.marketing_channel_pkg_info ci
              on uei.fchannel_code = ci.fid
            join dim.bpid_map bm
              on uei.fbpid = bm.fbpid
            where dt = '%(statdate)s'
        ) t
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fchannel_code, fhour
        grouping sets (
        (fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk, fhour),
        (fgamefsk,fplatformfsk,fhallfsk, fhour),
        (fgamefsk,fplatformfsk,fterminaltypefsk,fversionfsk, fhour),
        (fgamefsk,fplatformfsk, fhour))
        """

        res = self.sql_exe(hql)
        if res != 0:return res

        extend_group = {
            'fields':['hour(flts_at)+1'],
            'groups':[ [1], ]}

        hql = """
        select coalesce(fgamefsk,%(null_int_group_rule)d) fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
            coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
            hour(flts_at)+1 fhour,
            count(distinct fuid) fplayunum
        from dim.user_gameparty_stream
        where dt = '%(statdate)s'
        and hallmode=%(hallmode)s
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk,
        fchannel_code, hour(flts_at)+1
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        --活跃分时段玩牌人数
        drop table if exists work.user_action_hour_dis_gameparty_%(statdatenum)s;
        create table work.user_action_hour_dis_gameparty_%(statdatenum)s
        as
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        insert into table dcnew.user_action_hour_dis partition(dt = '%(statdate)s' )
        select
            '%(statdate)s' fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code fchannelcode,
            fdimension,
            max(case when fhour=1 then fnum else 0 end) fh1,
            max(case when fhour=2 then fnum else 0 end) fh2,
            max(case when fhour=3 then fnum else 0 end) fh3,
            max(case when fhour=4 then fnum else 0 end) fh4,
            max(case when fhour=5 then fnum else 0 end) fh5,
            max(case when fhour=6 then fnum else 0 end) fh6,
            max(case when fhour=7 then fnum else 0 end) fh7,
            max(case when fhour=8 then fnum else 0 end) fh8,
            max(case when fhour=9 then fnum else 0 end) fh9,
            max(case when fhour=10 then fnum else 0 end) fh10,
            max(case when fhour=11 then fnum else 0 end) fh11,
            max(case when fhour=12 then fnum else 0 end) fh12,
            max(case when fhour=13 then fnum else 0 end) fh13,
            max(case when fhour=14 then fnum else 0 end) fh14,
            max(case when fhour=15 then fnum else 0 end) fh15,
            max(case when fhour=16 then fnum else 0 end) fh16,
            max(case when fhour=17 then fnum else 0 end) fh17,
            max(case when fhour=18 then fnum else 0 end) fh18,
            max(case when fhour=19 then fnum else 0 end) fh19,
            max(case when fhour=20 then fnum else 0 end) fh20,
            max(case when fhour=21 then fnum else 0 end) fh21,
            max(case when fhour=22 then fnum else 0 end) fh22,
            max(case when fhour=23 then fnum else 0 end) fh23,
            max(case when fhour=24 then fnum else 0 end) fh24
        from (
            select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fchannel_code, fgame_id,
                    fhour, fplayunum fnum, 'dagptucnt' fdimension
            from work.user_action_hour_dis_gameparty_%(statdatenum)s
            union all
            select  fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fchannel_code, fgame_id,
                    fhour, flogincnt fnum, 'dalgncnt' fdimension
            from work.user_action_hour_dis_login_%(statdatenum)s
            union all
            select  fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fchannel_code, fgame_id,
                    fhour, floginunum fnum, 'dalgnucnt' fdimension
            from work.user_action_hour_dis_login_%(statdatenum)s
        ) a group by
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code,
            fdimension;

        drop table if exists work.user_action_hour_dis_login_%(statdatenum)s;
        drop table if exists work.user_action_hour_dis_gameparty_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res


a = agg_user_action_hour_dis(sys.argv[1:])
a()
