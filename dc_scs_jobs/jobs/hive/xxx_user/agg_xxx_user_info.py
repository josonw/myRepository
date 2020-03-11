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


class agg_xxx_user_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.xxx_user_info (
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
               freg_unum           bigint        comment '新增用户数',
               fact_unum           bigint        comment '活跃用户数',
               frupt_unum          bigint        comment '破产人数',
               frupt_num           bigint        comment '破产人次',
               frlv_unum           bigint        comment '救济人数',
               frlv_num            bigint        comment '救济人次'
               )comment 'xxx用户信息表'
               partitioned by(dt date)
        location '/dw/dcnew/xxx_user_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_type_id', 'fuid'],
                        'groups': [[1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--取当日新增活跃玩牌付费用户
                  drop table if exists work.xxx_user_info_tmp_a_%(statdatenum)s;
                create table work.xxx_user_info_tmp_a_%(statdatenum)s as
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
                 ,t1.fuid
            from dim.xxx_user t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt <= '%(statdate)s'
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
                       fuid,
                       count(1) unum
                  from work.xxx_user_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       fuid
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """drop table if exists work.xxx_user_info_tmp_%(statdatenum)s;
          create table work.xxx_user_info_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.xxx_user_info
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
           ,sum(freg_unum) freg_unum
           ,sum(fact_unum) fact_unum
           ,sum(frupt_unum) frupt_unum
           ,sum(frupt_num) frupt_num
           ,sum(frlv_unum) frlv_unum
           ,sum(frlv_num) frlv_num
    from (select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fgame_id fsubgamefsk
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fchannel_code
                 ,coalesce(t1.fuser_type,'ad') fuser_type
                 ,coalesce(t1.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,count(distinct t.fuid) freg_unum
                 ,0 fact_unum
                 ,0 frupt_unum
                 ,0 frupt_num
                 ,0 frlv_unum
                 ,0 frlv_num
            from dim.reg_user_array t
            left join work.xxx_user_info_tmp_%(statdatenum)s t1
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
                    fuser_type,
                    fuser_type_id
           union all
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fsubgamefsk
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fchannelcode
                 ,coalesce(t1.fuser_type,'ad') fuser_type
                 ,coalesce(t1.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,0 freg_unum
                 ,count(distinct t.fuid) fact_unum
                 ,0 frupt_unum
                 ,0 frupt_num
                 ,0 frlv_unum
                 ,0 frlv_num
            from dim.user_act_array t
            left join work.xxx_user_info_tmp_%(statdatenum)s t1
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
                    fuser_type,
                    fuser_type_id
           union all
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fsubgamefsk
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fchannelcode
                 ,coalesce(t1.fuser_type,'ad') fuser_type
                 ,coalesce(t1.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,0 freg_unum
                 ,0 fact_unum
                 ,count(distinct case when frupt_cnt > 0 then t.fuid end) frupt_unum
                 ,sum(frupt_cnt) frupt_num
                 ,count(distinct case when frlv_cnt > 0 then t.fuid end) frlv_unum
                 ,sum(frlv_cnt) frlv_num
            from dim.user_bankrupt_array t
            left join work.xxx_user_info_tmp_%(statdatenum)s t1
              on t.fuid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fchannelcode = t1.fchannel_code
             and t1.fgame_id = -21379
           where t.dt='%(statdate)s'
           group by t.fgamefsk,
                    t.fplatformfsk,
                    t.fhallfsk,
                    t.fsubgamefsk,
                    t.fterminaltypefsk,
                    t.fversionfsk,
                    t.fchannelcode,
                    fuser_type,
                    fuser_type_id) t
    group by fgamefsk,
             fplatformfsk,
             fhallfsk,
             fsubgamefsk,
             fterminaltypefsk,
             fversionfsk,
             fchannel_code,
             fuser_type,
             fuser_type_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.xxx_user_info_tmp_%(statdatenum)s;
                 drop table if exists work.xxx_user_info_tmp_a_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_xxx_user_info(sys.argv[1:])
a()
