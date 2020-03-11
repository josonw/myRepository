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


class agg_xxx_user_reg_actret_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.xxx_user_reg_actret_info (
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
               freg_date           date          comment '注册日期',
               freg_unum           bigint        comment '当日注册用户',
               fhall_ret_unum      bigint        comment '大厅留存'
               )comment 'xxx用户新增留存表'
               partitioned by(dt date)
        location '/dw/dcnew/xxx_user_reg_actret_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_type_id', 'flast_date', 'reg_uid'],
                        'groups': [[1, 1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--
                  drop table if exists work.xxx_user_reg_actret_info_tmp_a_%(statdatenum)s;
                create table work.xxx_user_reg_actret_info_tmp_a_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,coalesce(t3.fuser_type,'ad') fuser_type
                 ,coalesce(t3.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.fuid reg_uid
                 ,t1.dt flast_date
            from dim.reg_user_main_additional t1
            left join dim.xxx_user t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt <= '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
              or t1.dt='%(ld_14day_ago)s'
              or t1.dt='%(ld_30day_ago)s'
              or t1.dt='%(ld_60day_ago)s'
              or t1.dt='%(ld_90day_ago)s')
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
                       flast_date,
                       reg_uid,
                       count(1) num
                  from work.xxx_user_reg_actret_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       flast_date,
                       reg_uid
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """drop table if exists work.xxx_user_reg_actret_info_tmp_1_%(statdatenum)s;
          create table work.xxx_user_reg_actret_info_tmp_1_%(statdatenum)s as
            %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--
                  drop table if exists work.xxx_user_reg_actret_info_tmp_b_%(statdatenum)s;
                create table work.xxx_user_reg_actret_info_tmp_b_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,coalesce(t3.fuser_type,'ad') fuser_type
                 ,coalesce(t3.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.fuid reg_uid
                 ,t1.dt flast_date
            from dim.reg_user_sub t1
            left join dim.xxx_user t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt <= '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.fis_first = 1
             and ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
              or t1.dt='%(ld_14day_ago)s'
              or t1.dt='%(ld_30day_ago)s'
              or t1.dt='%(ld_60day_ago)s'
              or t1.dt='%(ld_90day_ago)s');
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
                       flast_date,
                       reg_uid,
                       count(1) num
                  from work.xxx_user_reg_actret_info_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       flast_date,
                       reg_uid
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """drop table if exists work.xxx_user_reg_actret_info_tmp_2_%(statdatenum)s;
          create table work.xxx_user_reg_actret_info_tmp_2_%(statdatenum)s as
            %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.xxx_user_reg_actret_info
        partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate,
                 t.fgamefsk,
                 t.fplatformfsk,
                 t.fhallfsk,
                 t.fgame_id,
                 t.fterminaltypefsk,
                 t.fversionfsk,
                 t.fchannel_code,
                 t.fuser_type,
                 t.fuser_type_id,
                 t.flast_date,
                 count(distinct reg_uid) freg_unum,
                 count(distinct fuid) fhall_ret_unum
            from work.xxx_user_reg_actret_info_tmp_1_%(statdatenum)s t
            left join dim.user_act_array t1
              on t.reg_uid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fgame_id = t1.fsubgamefsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fchannel_code = t1.fchannelcode
             and t1.dt='%(statdate)s'
           where t.fgame_id = -21379
           group by t.fgamefsk,
                    t.fplatformfsk,
                    t.fhallfsk,
                    t.fgame_id,
                    t.fterminaltypefsk,
                    t.fversionfsk,
                    t.fchannel_code,
                    t.fuser_type,
                    t.fuser_type_id,
                    t.flast_date;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert into table dcnew.xxx_user_reg_actret_info
        partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate,
                 t.fgamefsk,
                 t.fplatformfsk,
                 t.fhallfsk,
                 t.fgame_id,
                 t.fterminaltypefsk,
                 t.fversionfsk,
                 t.fchannel_code,
                 t.fuser_type,
                 t.fuser_type_id,
                 t.flast_date,
                 count(distinct reg_uid) freg_unum,
                 count(distinct fuid) fhall_ret_unum
            from work.xxx_user_reg_actret_info_tmp_2_%(statdatenum)s t
            left join dim.user_act_array t1
              on t.reg_uid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fgame_id = t1.fsubgamefsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fchannel_code = t1.fchannelcode
             and t1.dt='%(statdate)s'
           where t.fgame_id <> -21379
           group by t.fgamefsk,
                    t.fplatformfsk,
                    t.fhallfsk,
                    t.fgame_id,
                    t.fterminaltypefsk,
                    t.fversionfsk,
                    t.fchannel_code,
                    t.fuser_type,
                    t.fuser_type_id,
                    t.flast_date;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.xxx_user_reg_actret_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.xxx_user_reg_actret_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.xxx_user_reg_actret_info_tmp_a_%(statdatenum)s;
                 drop table if exists work.xxx_user_reg_actret_info_tmp_b_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_xxx_user_reg_actret_info(sys.argv[1:])
a()
