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


class agg_bud_user_coord_address(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_coord_address (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fcode                 varchar(100)   comment '地区代码',
               fnum                  bigint         comment '全量用户数',
               f1d_num               bigint         comment '1日活跃用户数',
               f3d_num               bigint         comment '3日活跃用户数',
               f7d_num               bigint         comment '7日活跃用户数',
               f14d_num              bigint         comment '14日活跃用户数',
               f30d_num              bigint         comment '30日活跃用户数'
               ) comment '用户经纬度信息'
        partitioned by(dt date)
        location '/dw/bud_dm/bud_user_coord_address';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fcode'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_coord_address_tmp_1_%(statdatenum)s;
          create table work.bud_user_coord_address_tmp_1_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,-13658 fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,1 hallmode
                 ,fc_country
                 ,fc_province
                 ,fc_city
                 ,fc_district
                 ,fc_street
                 ,fuid
                 ,fplatform_uid
                 ,fdate
                 ,flatitude
                 ,flongitude
            from dim.user_coord_address t1
           where t1.dt = '%(statdate)s'
             and t1.fgamefsk = 4132314431
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """--区县
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fcode
                 ,count(distinct fuid) fnum
                 ,count(distinct case when to_date(fdate) = '%(statdate)s'  then fuid end) f1d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',3) and '%(statdate)s'  then fuid end) f3d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',7) and '%(statdate)s'  then fuid end) f7d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',14) and '%(statdate)s'  then fuid end) f14d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',30) and '%(statdate)s'  then fuid end) f30d_num
            from work.bud_user_coord_address_tmp_1_%(statdatenum)s t
            join bud_dm.region_dim_info t1
              on t.fc_district = t1.fname_4
             and t1.flevel = 4
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fcode
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_coord_address
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """--市
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fcode
                 ,count(distinct fuid) fnum
                 ,count(distinct case when to_date(fdate) = '%(statdate)s'  then fuid end) f1d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',3) and '%(statdate)s'  then fuid end) f3d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',7) and '%(statdate)s'  then fuid end) f7d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',14) and '%(statdate)s'  then fuid end) f14d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',30) and '%(statdate)s'  then fuid end) f30d_num
            from work.bud_user_coord_address_tmp_1_%(statdatenum)s t
            join bud_dm.region_dim_info t1
              on t.fc_city = t1.fname_3
             and t1.flevel = 3
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fcode
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert into table bud_dm.bud_user_coord_address
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """--省
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fcode
                 ,count(distinct fuid) fnum
                 ,count(distinct case when to_date(fdate) = '%(statdate)s'  then fuid end) f1d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',3) and '%(statdate)s'  then fuid end) f3d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',7) and '%(statdate)s'  then fuid end) f7d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',14) and '%(statdate)s'  then fuid end) f14d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',30) and '%(statdate)s'  then fuid end) f30d_num
            from work.bud_user_coord_address_tmp_1_%(statdatenum)s t
            join bud_dm.region_dim_info t1
              on t.fc_province = t1.fname_2
             and t1.flevel = 2
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fcode
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert into table bud_dm.bud_user_coord_address
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """--区县
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fcode
                 ,count(distinct fuid) fnum
                 ,count(distinct case when to_date(fdate) = '%(statdate)s'  then fuid end) f1d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',3) and '%(statdate)s'  then fuid end) f3d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',7) and '%(statdate)s'  then fuid end) f7d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',14) and '%(statdate)s'  then fuid end) f14d_num
                 ,count(distinct case when to_date(fdate) between date_sub('%(statdate)s',30) and '%(statdate)s'  then fuid end) f30d_num
            from work.bud_user_coord_address_tmp_1_%(statdatenum)s t
            join bud_dm.region_dim_info t1
              on t.fc_country = t1.fname_1
             and t1.flevel = 1
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fcode
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert into table bud_dm.bud_user_coord_address
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_coord_address_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_coord_address(sys.argv[1:])
a()
