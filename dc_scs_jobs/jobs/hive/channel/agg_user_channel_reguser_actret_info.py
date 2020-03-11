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


class agg_user_channel_reguser_actret_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_channel_reguser_actret_info (
               fdate                  date,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fchannel_id            bigint   comment '渠道id',
               flast_date             date     comment '注册日期',
               freg_unum              bigint   comment '当日注册用户',
               fhall_ret_unum         bigint   comment '大厅留存'
               )comment '渠道注册留存'
               partitioned by(dt date)
        location '/dw/dcnew/user_channel_reguser_actret_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fchannel_id', 'flast_date'],
                        'groups': [[1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--新增用户
                  drop table if exists work.user_channel_reguser_actret_info_tmp_a_%(statdatenum)s;
                create table work.user_channel_reguser_actret_info_tmp_a_%(statdatenum)s as
          select distinct c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fversionfsk,
                 %(null_int_report)d fgame_id,
                 %(null_int_report)d fchannel_code,
                 c.hallmode,
                 b.fpkg_channel_id fchannel_id,
                 a.fuid reg_uid,
                 t2.fuid act_uid,
                 a.dt flast_date
            from stage.channel_market_reg_mid a
            left join dim.user_act t2
              on a.fbpid = t2.fbpid
             and a.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            join analysis.dc_channel_package b
              on a.fnow_channel_id = b.fpkg_id
            join dim.bpid_map c
              on a.fbpid=c.fbpid
           where ((a.dt >= '%(ld_7day_ago)s' and a.dt <= '%(statdate)s')
              or a.dt='%(ld_14day_ago)s'
              or a.dt='%(ld_30day_ago)s'
              or a.dt='%(ld_60day_ago)s'
              or a.dt='%(ld_90day_ago)s');
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--新增
                select  "%(statdate)s" fdate
                       ,fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,flast_date
                       ,count(distinct reg_uid) freg_unum
                       ,count(distinct act_uid) fhall_ret_unum
                  from work.user_channel_reguser_actret_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id,flast_date
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """ insert overwrite table dcnew.user_channel_reguser_actret_info partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_channel_reguser_actret_info_tmp_a_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_channel_reguser_actret_info(sys.argv[1:])
a()
