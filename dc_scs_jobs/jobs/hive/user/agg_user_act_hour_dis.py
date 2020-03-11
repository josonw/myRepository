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


class agg_user_act_hour_dis(BaseStatModel):
    def create_tab(self):

        hql = """--活跃用户时段分布
        create table if not exists dcnew.user_act_hour_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fhourfsk            int        comment '游戏时段:1-24',
               fact_unum           bigint     comment '活跃人数'
               )comment '活跃用户时段分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_act_hour_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fhourfsk'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """--汇总登录牌局金流和子游戏登陆数据
        drop table if exists work.user_act_hour_dis_tmp_%(statdatenum)s;
        create table work.user_act_hour_dis_tmp_%(statdatenum)s as
            select distinct bm.fgamefsk,
                   bm.fplatformfsk,
                   bm.fhallfsk,
                   bm.fterminaltypefsk,
                   bm.fversionfsk,
                   bm.hallmode,
                   us.fgame_id,
                   us.fchannel_code,
                   fhourfsk,
                   fuid
              from (select fbpid,
                           fuid,
                           hour(flts_at)+1 fhourfsk,
                           coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                           coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code
                      from stage.user_enter_stg us --子游戏登陆数据
                     where dt = '%(statdate)s'
                       and fgame_id is not null
                     union
                    select fbpid,
                           fuid,
                           hour(flts_at)+1 fhourfsk,
                           coalesce(fgame_id,%(null_int_report)d) fgame_id,
                           coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code
                      from dim.user_gameparty_stream us --牌局数据
                     where dt = '%(statdate)s'
                     union
                    select fbpid,
                           fuid,
                           hour(flogin_at)+1 fhourfsk,
                           %(null_int_report)d fgame_id,
                           coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code
                      from dim.user_login_additional us --大厅登陆数据
                     where dt = '%(statdate)s'
                     union
                    select fbpid,
                           fuid,
                           hour(lts_at)+1 fhourfsk,
                           coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                           coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code
                      from stage.pb_gamecoins_stream_stg us --金流数据
                     where dt = '%(statdate)s'
                   ) us
              join dim.bpid_map bm
                on us.fbpid = bm.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到临时表上
                select '%(statdate)s' fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fhourfsk,
                       count(distinct fuid) fact_unum
                  from work.user_act_hour_dis_tmp_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fhourfsk

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.user_act_hour_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_act_hour_dis_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = agg_user_act_hour_dis(sys.argv[1:])
a()
