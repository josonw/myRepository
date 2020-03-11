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

from libs.ImpalaSql import ImpalaSql

# 对新增付费用户经纬度去重后直接使用
# 对活跃用户非中国的经纬度去重后直接使用，中国的经纬度去重后根据经纬度分桶取八分之一

class load_user_reg_act_pay_user_country(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists dim.user_reg_act_pay_user_country (
               fdate                 string,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fuid                  bigint,
               fis_reg               bigint          comment '是否新增',
               fis_act               bigint          comment '是否活跃',
               fis_pay               bigint          comment '是否付费',
               fc_country            string          comment '国家',
               fc_province           string          comment '省',
               fc_city               string          comment '市',
               fc_district           string          comment '区',
               fc_street             string          comment '路',
               flatitude             double          comment '经度',
               flongitude            double          comment '纬度',
               level                 int             comment '级别：1 国家 2 省'
               ) comment '当天新增活跃付费用户'
        partitioned by (dt string)
        stored as parquet;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1
        statdate = self.stat_date
        query = {'statdate': statdate,
                 'statdatenum': statdate.replace("-", "")}

        res = self.sql_exe("""set mapreduce.map.memory.mb=2048;set mapreduce.map.java.opts=-Xmx1700m;""")
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_reg_act_pay_user_tmp_%(statdatenum)s;
          create table work.user_reg_act_pay_user_tmp_%(statdatenum)s as
          select  fgamefsk
                  ,max(fplatformfsk) fplatformfsk
                  ,max(fhallfsk) fhallfsk
                  ,max(fsubgamefsk) fsubgamefsk
                  ,max(fterminaltypefsk) fterminaltypefsk
                  ,max(fversionfsk) fversionfsk
                  ,max(fuid) fuid
                  ,0 fis_reg
                  ,1 fis_act
                  ,0 fis_pay
                  ,max(fc_country) fc_country
                  ,max(fc_province) fc_province
                  ,max(fc_city) fc_city
                  ,max(fc_district) fc_district
                  ,max(fc_street) fc_street
                  ,flatitude
                  ,flongitude
            from dim.user_reg_act_pay_user
           where dt = '%(statdate)s'
             and flatitude is not null
             and fis_act = 1
             group by fgamefsk
                      ,flatitude
                      ,flongitude
             distribute by flatitude,flongitude;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_reg_act_pay_user_tmp_1_%(statdatenum)s;
          create table work.user_reg_act_pay_user_tmp_1_%(statdatenum)s as
          select  fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,fsubgamefsk
                  ,fterminaltypefsk
                  ,fversionfsk
                  ,fuid
                  ,fis_reg
                  ,fis_act
                  ,fis_pay
                  ,fc_country
                  ,fc_province
                  ,fc_city
                  ,fc_district
                  ,fc_street
                  ,flatitude
                  ,flongitude
            from work.user_reg_act_pay_user_tmp_%(statdatenum)s tablesample(bucket 1 out of 8 on flatitude,flongitude)
           where fc_country = '中国'
           union all
          select  fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,fsubgamefsk
                  ,fterminaltypefsk
                  ,fversionfsk
                  ,fuid
                  ,fis_reg
                  ,fis_act
                  ,fis_pay
                  ,fc_country
                  ,fc_province
                  ,fc_city
                  ,fc_district
                  ,fc_street
                  ,flatitude
                  ,flongitude
            from work.user_reg_act_pay_user_tmp_%(statdatenum)s
           where fc_country <> '中国';
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = ("""--全量用户
                invalidate metadata;
            insert overwrite table dim.user_reg_act_pay_user_country partition(dt='%(statdate)s')
          select  '%(statdate)s' fdate
                  ,t.fgamefsk
                  ,max(t.fplatformfsk) fplatformfsk
                  ,max(t.fhallfsk) fhallfsk
                  ,max(t.fsubgamefsk) fsubgamefsk
                  ,max(t.fterminaltypefsk) fterminaltypefsk
                  ,max(t.fversionfsk) fversionfsk
                  ,max(t.fuid) fuid
                  ,1 fis_reg
                  ,0 fis_act
                  ,0 fis_pay
                  ,max(t.fc_country) fc_country
                  ,max(t.fc_province) fc_province
                  ,max(t.fc_city) fc_city
                  ,max(t.fc_district) fc_district
                  ,max(t.fc_street) fc_street
                  ,t.flatitude
                  ,t.flongitude
                  ,1 level
            from dim.user_reg_act_pay_user t
           where t.dt = '%(statdate)s'
             and flatitude is not null
             and fis_reg = 1
             group by t.fgamefsk
                      ,t.flatitude
                      ,t.flongitude
           union all
          select  '%(statdate)s' fdate
                  ,t.fgamefsk
                  ,max(t.fplatformfsk) fplatformfsk
                  ,max(t.fhallfsk) fhallfsk
                  ,max(t.fsubgamefsk) fsubgamefsk
                  ,max(t.fterminaltypefsk) fterminaltypefsk
                  ,max(t.fversionfsk) fversionfsk
                  ,max(t.fuid) fuid
                  ,0 fis_reg
                  ,0 fis_act
                  ,1 fis_pay
                  ,max(t.fc_country) fc_country
                  ,max(t.fc_province) fc_province
                  ,max(t.fc_city) fc_city
                  ,max(t.fc_district) fc_district
                  ,max(t.fc_street) fc_street
                  ,t.flatitude
                  ,t.flongitude
                  ,1 level
            from dim.user_reg_act_pay_user t
           where t.dt = '%(statdate)s'
             and flatitude is not null
             and fis_pay = 1
             group by t.fgamefsk
                      ,t.flatitude
                      ,t.flongitude
           union all
          select  '%(statdate)s' fdate
                  ,fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,fsubgamefsk
                  ,fterminaltypefsk
                  ,fversionfsk
                  ,fuid
                  ,fis_reg
                  ,fis_act
                  ,fis_pay
                  ,fc_country
                  ,fc_province
                  ,fc_city
                  ,fc_district
                  ,fc_street
                  ,flatitude
                  ,flongitude
                  ,1 level
            from work.user_reg_act_pay_user_tmp_1_%(statdatenum)s ;
                invalidate metadata;
        """) % query
        impala = ImpalaSql(host='10.30.101.104')
        res = impala.exe_sql(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = ("""--全量用户
                invalidate metadata;
            insert into table dim.user_reg_act_pay_user_country partition(dt='%(statdate)s')
          select  '%(statdate)s' fdate
                  ,t.fgamefsk
                  ,max(t.fplatformfsk) fplatformfsk
                  ,max(t.fhallfsk) fhallfsk
                  ,max(t.fsubgamefsk) fsubgamefsk
                  ,max(t.fterminaltypefsk) fterminaltypefsk
                  ,max(t.fversionfsk) fversionfsk
                  ,max(t.fuid) fuid
                  ,1 fis_reg
                  ,0 fis_act
                  ,0 fis_pay
                  ,max(t.fc_country) fc_country
                  ,max(t.fc_province) fc_province
                  ,max(t.fc_city) fc_city
                  ,max(t.fc_district) fc_district
                  ,max(t.fc_street) fc_street
                  ,t.flatitude
                  ,t.flongitude
                  ,2 level
            from dim.user_reg_act_pay_user t
           where t.dt = '%(statdate)s'
             and flatitude is not null
             and fis_reg = 1
             group by t.fgamefsk
                      ,t.flatitude
                      ,t.flongitude
           union all
          select  '%(statdate)s' fdate
                  ,t.fgamefsk
                  ,max(t.fplatformfsk) fplatformfsk
                  ,max(t.fhallfsk) fhallfsk
                  ,max(t.fsubgamefsk) fsubgamefsk
                  ,max(t.fterminaltypefsk) fterminaltypefsk
                  ,max(t.fversionfsk) fversionfsk
                  ,max(t.fuid) fuid
                  ,0 fis_reg
                  ,0 fis_act
                  ,1 fis_pay
                  ,max(t.fc_country) fc_country
                  ,max(t.fc_province) fc_province
                  ,max(t.fc_city) fc_city
                  ,max(t.fc_district) fc_district
                  ,max(t.fc_street) fc_street
                  ,t.flatitude
                  ,t.flongitude
                  ,2 level
            from dim.user_reg_act_pay_user t
           where t.dt = '%(statdate)s'
             and flatitude is not null
             and fis_pay = 1
             group by t.fgamefsk
                      ,t.flatitude
                      ,t.flongitude
           union all
          select  '%(statdate)s' fdate
                  ,t.fgamefsk
                  ,max(t.fplatformfsk) fplatformfsk
                  ,max(t.fhallfsk) fhallfsk
                  ,max(t.fsubgamefsk) fsubgamefsk
                  ,max(t.fterminaltypefsk) fterminaltypefsk
                  ,max(t.fversionfsk) fversionfsk
                  ,max(t.fuid) fuid
                  ,0 fis_reg
                  ,1 fis_act
                  ,0 fis_pay
                  ,max(t.fc_country) fc_country
                  ,max(t.fc_province) fc_province
                  ,max(t.fc_city) fc_city
                  ,max(t.fc_district) fc_district
                  ,max(t.fc_street) fc_street
                  ,t.flatitude
                  ,t.flongitude
                  ,2 level
            from dim.user_reg_act_pay_user t
           where t.dt = '%(statdate)s'
             and flatitude is not null
             and fis_act = 1
             group by t.fgamefsk
                      ,t.flatitude
                      ,t.flongitude;
                invalidate metadata;
        """) % query
        impala = ImpalaSql(host='10.30.101.104')
        res = impala.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_reg_act_pay_user_tmp_%(statdatenum)s;
                 drop table if exists work.user_reg_act_pay_user_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_reg_act_pay_user_country(sys.argv[1:])
a()
