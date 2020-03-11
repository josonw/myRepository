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


class load_user_reg_act_pay_user(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists dim.user_reg_act_pay_user (
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
               flongitude            double          comment '纬度'
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

        # 取基础数据
        hql = """--当天新增活跃付费
            drop table if exists work.bud_user_act_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_act_info_tmp_1_%(statdatenum)s as
          select  fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fuid
                 ,max(fis_reg) fis_reg
                 ,max(fis_act) fis_act
                 ,max(fis_pay) fis_pay
            from (select  tt.fgamefsk
                         ,tt.fplatformfsk
                         ,tt.fhallfsk
                         ,t1.fgame_id
                         ,tt.fterminaltypefsk
                         ,tt.fversionfsk
                         ,t1.fuid
                         ,0 fis_reg
                         ,1 fis_act
                         ,0 fis_pay
                    from dim.user_act t1
                    join dim.bpid_map tt
                      on t1.fbpid = tt.fbpid
                   where t1.dt = '%(statdate)s'
                   union all
                  select  tt.fgamefsk
                         ,tt.fplatformfsk
                         ,tt.fhallfsk
                         ,-13658 fgame_id
                         ,tt.fterminaltypefsk
                         ,tt.fversionfsk
                         ,t1.fuid
                         ,1 fis_reg
                         ,0 fis_act
                         ,0 fis_pay
                    from dim.reg_user_main t1
                    join dim.bpid_map tt
                      on t1.fbpid = tt.fbpid
                   where t1.dt = '%(statdate)s'
                   union all
                  select  tt.fgamefsk
                         ,tt.fplatformfsk
                         ,tt.fhallfsk
                         ,-13658 fgame_id
                         ,tt.fterminaltypefsk
                         ,tt.fversionfsk
                         ,t1.fuid
                         ,0 fis_reg
                         ,0 fis_act
                         ,1 fis_pay
                    from dim.user_pay_day t1
                    join dim.bpid_map tt
                      on t1.fbpid = tt.fbpid
                   where t1.dt = '%(statdate)s'
                 ) t
           group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fgame_id
                    ,fterminaltypefsk
                    ,fversionfsk
                    ,fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--当天新增活跃付费
            drop table if exists work.bud_user_act_info_tmp_2_%(statdatenum)s;
          create table work.bud_user_act_info_tmp_2_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,fuid
                 ,fc_country
                 ,fc_province
                 ,fc_city
                 ,cast (flatitude as double) flatitude
                 ,cast (flongitude as double) flongitude
            from (select fbpid
                         ,fip_country fc_country
                         ,fip_province fc_province
                         ,fip_city fc_city
                         ,fuid
                         ,flogin_at
                         ,fip_latitude flatitude
                         ,fip_longitude flongitude
                         ,row_number() over(partition by fbpid, fuid order by flogin_at desc, fip_latitude desc, fip_longitude desc) row_num
                    from stage.user_login_stg t1
                   where t1.dt = '%(statdate)s' ) t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.row_num = 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = ("""--全量用户
                invalidate metadata;
            insert overwrite table dim.user_reg_act_pay_user partition(dt='%(statdate)s')
          select  '%(statdate)s' fdate
                  ,t1.fgamefsk
                  ,t1.fplatformfsk
                  ,t1.fhallfsk
                  ,t1.fgame_id fsubgamefsk
                  ,t1.fterminaltypefsk
                  ,t1.fversionfsk
                  ,t1.fuid
                  ,t1.fis_reg
                  ,t1.fis_act
                  ,t1.fis_pay
                  ,cast (coalesce(t2.fc_country, t3.fc_country) as string) fc_country
                  ,cast (coalesce(t2.fc_province, t3.fc_province) as string) fc_province
                  ,cast (coalesce(t2.fc_city, t3.fc_city) as string) fc_city
                  ,cast (t2.fc_district as string) fc_district
                  ,cast (t2.fc_street as string) fc_street
                  ,coalesce(t2.flatitude, t3.flatitude) flatitude
                  ,coalesce(t2.flongitude, t3.flongitude) flongitude
            from work.bud_user_act_info_tmp_1_%(statdatenum)s t1
            left join dim.user_coord_address_day t2
              on t1.fgamefsk = t2.fgamefsk
             and t1.fplatformfsk = t2.fplatformfsk
             and t1.fhallfsk = t2.fhallfsk
             and t1.fterminaltypefsk = t2.fterminaltypefsk
             and t1.fversionfsk = t2.fversionfsk
             and t1.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            left join work.bud_user_act_info_tmp_2_%(statdatenum)s t3
              on t1.fgamefsk = t3.fgamefsk
             and t1.fplatformfsk = t3.fplatformfsk
             and t1.fhallfsk = t3.fhallfsk
             and t1.fterminaltypefsk = t3.fterminaltypefsk
             and t1.fversionfsk = t3.fversionfsk
             and t1.fuid = t3.fuid;
                invalidate metadata;
        """) % query
        impala = ImpalaSql(host='10.30.101.104')
        res = impala.exe_sql(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_reg_act_pay_user(sys.argv[1:])
a()
