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


class agg_xxx_user_loss_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.xxx_user_loss_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fuser_type          varchar(10)   comment '用户类型：ad等',
               fuser_type_id       varchar(100)  comment '用户类型id',
               flossdays           int           comment '流失天数',
               floss_unum          bigint        comment '对应流失用户人数'
               )comment 'xxx用户流失回流表'
               partitioned by(dt date)
        location '/dw/dcnew/xxx_user_loss_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_type_id', 'flossdays'],
                        'groups': [[1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--
                  drop table if exists work.xxx_user_loss_info_tmp_a_%(statdatenum)s;
                create table work.xxx_user_loss_info_tmp_a_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,coalesce(t3.fuser_type,'ad') fuser_type
                 ,coalesce(t3.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.fuid
                 ,t1.days flossdays
            from dim.user_churn t1
            left join dim.xxx_user t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt <= '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       flossdays,
                       count(distinct fuid) floss_unum
                  from work.xxx_user_loss_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       flossdays
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """insert overwrite table dcnew.xxx_user_loss_info partition(dt='%(statdate)s')
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.xxx_user_loss_info_tmp_a_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_xxx_user_loss_info(sys.argv[1:])
a()
