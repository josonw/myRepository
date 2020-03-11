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


class agg_bud_push_user_act_hour_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_push_user_act_hour_dis (
               fdate               date,
               fpush_id            bigint        comment '推送id',
               fhourfsk            bigint        comment '时段',
               fact_unum           bigint        comment '活跃用户'
               )comment '推送用户当天活跃时段分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_push_user_act_hour_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """--当天算两天前的数据
        drop table if exists work.bud_push_user_act_hour_dis_tmp_1_%(statdatenum)s;
        create table work.bud_push_user_act_hour_dis_tmp_1_%(statdatenum)s as
            select distinct t2.fbpid, t1.fuid, t2.fpush_id
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

        # 汇总1
        hql = """insert overwrite table bud_dm.bud_push_user_act_hour_dis
            partition(dt='%(ld_1day_ago)s')
            select '%(ld_1day_ago)s' fdate, fpush_id,fhourfsk,count(distinct fuid) fact_unum
              from (select  t1.fpush_id
                           ,hour(t2.flogin_at)+1 fhourfsk
                           ,t1.fuid
                      from work.bud_push_user_act_hour_dis_tmp_1_%(statdatenum)s t1
                      join dim.user_login_additional t2
                        on t1.fbpid = t2.fbpid
                       and t1.fuid = t2.fuid
                       and t2.dt = '%(ld_1day_ago)s'
                     union all
                    --玩牌
                    select  t1.fpush_id
                           ,hour(t2.flts_at)+1 fhourfsk
                           ,t1.fuid
                      from work.bud_push_user_act_hour_dis_tmp_1_%(statdatenum)s t1
                      join stage.user_gameparty_stg t2
                        on t1.fbpid = t2.fbpid
                       and t1.fuid = t2.fuid
                       and t2.dt = '%(ld_1day_ago)s'
                     union all
                    --金流
                    select  t1.fpush_id
                           ,hour(t2.lts_at)+1 fhourfsk
                           ,t1.fuid
                      from work.bud_push_user_act_hour_dis_tmp_1_%(statdatenum)s t1
                      join stage.pb_gamecoins_stream_stg t2
                        on t1.fbpid = t2.fbpid
                       and t1.fuid = t2.fuid
                       and t2.dt = '%(ld_1day_ago)s'
                   ) t
             group by fpush_id,fhourfsk;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_push_user_act_hour_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_push_user_act_hour_dis(sys.argv[1:])
a()
