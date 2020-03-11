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


class agg_xxx_channel_user_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.xxx_channel_user_info (
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
               fchannel_id         varchar(100)  comment '渠道包编号',
               freg_unum           bigint        comment '新增用户数',
               fpay_unum           bigint        comment '付费用户数',
               fpay_income         decimal(20,2) comment '付费金额'
               )comment 'xxx用户渠道信息表'
               partitioned by(dt date)
        location '/dw/dcnew/xxx_channel_user_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_type_id', 'fuid', 'fchannel_id'],
                        'groups': [[1, 1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--取当日新增活跃玩牌付费用户
                  drop table if exists work.xxx_channel_user_info_tmp_a_%(statdatenum)s;
                create table work.xxx_channel_user_info_tmp_a_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,coalesce(t1.fuser_type,'ad') fuser_type
                 ,coalesce(t1.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.fchannel_id
                 ,t1.fuid
            from dim.xxx_user t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt <= '%(statdate)s' and fchannel_id is not null
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
                       fchannel_id,
                       fuid,
                       count(1) unum
                  from work.xxx_channel_user_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       fuid,
                       fchannel_id
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """drop table if exists work.xxx_channel_user_info_tmp_%(statdatenum)s;
          create table work.xxx_channel_user_info_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.xxx_channel_user_info
        partition( dt="%(statdate)s" )
   select "%(statdate)s" fdate
           ,t.fgamefsk
           ,t.fplatformfsk
           ,t.fhallfsk
           ,t.fsubgamefsk
           ,t.fterminaltypefsk
           ,t.fversionfsk
           ,t.fchannel_code
           ,t.fuser_type
           ,t.fuser_type_id
           ,t.fchannel_id
           ,sum(freg_unum) freg_unum
           ,sum(fpay_unum) fpay_unum
           ,sum(fpay_income) fpay_income
    from (select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fgame_id fsubgamefsk
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fchannel_code
                 ,t1.fuser_type
                 ,t1.fuser_type_id
                 ,t1.fchannel_id
                 ,count(distinct t.fuid) freg_unum
                 ,0 fpay_unum
                 ,0 fpay_income
            from dim.reg_user_array t
            join work.xxx_channel_user_info_tmp_%(statdatenum)s t1
              on t.fuid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fchannel_code = t1.fchannel_code
           where t.dt='%(statdate)s'
           group by t.fgamefsk,
                    t.fplatformfsk,
                    t.fhallfsk,
                    t.fgame_id,
                    t.fterminaltypefsk,
                    t.fversionfsk,
                    t.fchannel_code,
                    t1.fuser_type,
                    t1.fuser_type_id,
                    t1.fchannel_id
           union all
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fsubgamefsk
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fchannelcode
                 ,t1.fuser_type
                 ,t1.fuser_type_id
                 ,t1.fchannel_id
                 ,0 freg_unum
                 ,count(distinct t.fuid) fpay_unum
                 ,sum(ftotal_usd_amt) fpay_income
            from dim.user_pay_array t
            join work.xxx_channel_user_info_tmp_%(statdatenum)s t1
              on t.fuid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fchannelcode = t1.fchannel_code
           where t.dt='%(statdate)s'
           group by t.fgamefsk,
                    t.fplatformfsk,
                    t.fhallfsk,
                    t.fsubgamefsk,
                    t.fterminaltypefsk,
                    t.fversionfsk,
                    t.fchannelcode,
                    t1.fuser_type,
                    t1.fuser_type_id,
                    t1.fchannel_id) t
    group by fgamefsk,
             fplatformfsk,
             fhallfsk,
             fsubgamefsk,
             fterminaltypefsk,
             fversionfsk,
             fchannel_code,
             fuser_type,
             fuser_type_id,
             fchannel_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.xxx_channel_user_info_tmp_%(statdatenum)s;
                 drop table if exists work.xxx_channel_user_info_tmp_a_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_xxx_channel_user_info(sys.argv[1:])
a()
