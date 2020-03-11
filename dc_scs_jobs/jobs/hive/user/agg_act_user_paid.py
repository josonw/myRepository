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


class agg_act_user_paid(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists dcnew.act_user_paid (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fdaucnt bigint comment '当天活跃总人数',
                fdhpucnt bigint comment '当天历史付费总人数'
               )comment '历史付费'
               partitioned by(dt date)
        location '/dw/dcnew/act_user_paid';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取订单相关指标
            drop table if exists work.act_user_paid_tmp_%(statdatenum)s;
          create table work.act_user_paid_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ bm.fgamefsk
                 ,bm.fplatformfsk
                 ,bm.fhallfsk
                 ,ua.fgame_id
                 ,bm.fterminaltypefsk
                 ,bm.fversionfsk
                 ,bm.hallmode
                 ,ua.fuid
                 ,pi.fuid fpay_uid
            from dim.user_act ua
            left join dim.user_act_paid pi
              on ua.fbpid = pi.fbpid
             and ua.fuid = pi.fuid
             and pi.dt = '%(statdate)s'
            join dim.bpid_map bm
            on ua.fbpid = bm.fbpid
            where ua.dt = '%(statdate)s'
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
                 ,%(null_int_group_rule)d fchannelcode
                 ,count(distinct fuid) fdaucnt
                 ,count(distinct fpay_uid) fdhpucnt
            from work.act_user_paid_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """
        self.sql_template_build(sql=hql)

        hql = """
         insert overwrite table dcnew.act_user_paid
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.act_user_paid_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_act_user_paid(sys.argv[1:])
a()
