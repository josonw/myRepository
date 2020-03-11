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


class load_user_coord_address(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists dim.user_coord_address_day (
               fdate                 varchar(100),
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fuid                  bigint,
               fplatform_uid         bigint,
               fc_country            varchar(100)    comment '国家',
               fc_province           varchar(100)    comment '省',
               fc_city               varchar(100)    comment '市',
               fc_district           varchar(100)    comment '区',
               fc_street             varchar(100)    comment '路',
               flatitude             double          comment '经度',
               flongitude            double          comment '纬度'
               ) comment '当天活跃用户最后一次经纬度信息'
        partitioned by (dt varchar(100))
        stored as parquet;


        create table if not exists dim.user_coord_address (
               fdate                 varchar(100),
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fuid                  bigint,
               fplatform_uid         bigint,
               fc_country            varchar(100)    comment '国家',
               fc_province           varchar(100)    comment '省',
               fc_city               varchar(100)    comment '市',
               fc_district           varchar(100)    comment '区',
               fc_street             varchar(100)    comment '路',
               flatitude             double          comment '经度',
               flongitude            double          comment '纬度'
               ) comment '全量用户最后一次经纬度信息'
        partitioned by (dt varchar(100))
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

        res = self.sql_exe("""set mapreduce.map.memory.mb=2048;set mapreduce.map.java.opts=-Xmx1700m;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--当天活跃
            insert overwrite table dim.user_coord_address_day partition(dt='%(statdate)s')
          select  flts_at fdate
                 ,tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,-13658 fsubgamefsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,fuid
                 ,fplatform_uid
                 ,fc_country
                 ,fc_province
                 ,fc_city
                 ,fc_district
                 ,fc_street
                 ,cast (flatitude as double) flatitude
                 ,cast (flongitude as double) flongitude
            from (select fbpid
                         ,fc_country
                         ,fc_province
                         ,fc_city
                         ,fc_district
                         ,fc_street
                         ,fuid
                         ,fplatform_uid
                         ,flts_at
                         ,flatitude
                         ,flongitude
                         ,row_number() over(partition by fbpid, fuid order by flts_at desc, flatitude desc, flongitude desc) row_num
                    from stage.x_user_coord_address_stg t1
                   where t1.dt = '%(statdate)s' ) t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.row_num = 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--全量用户
            insert overwrite table dim.user_coord_address partition(dt='%(statdate)s')
          select  fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fsubgamefsk
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fuid
                 ,fplatform_uid
                 ,fc_country
                 ,fc_province
                 ,fc_city
                 ,fc_district
                 ,fc_street
                 ,flatitude
                 ,flongitude
            from (select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,
                         fuid,fplatform_uid,fc_country,fc_province,fc_city,fc_district,fc_street,flatitude,flongitude,
                         row_number() over(partition by fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fuid
                                           order by fdate desc, flatitude desc, flongitude desc) rown
                    from (select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,
                                 fuid,fplatform_uid,fc_country,fc_province,fc_city,fc_district,fc_street,flatitude,flongitude
                            from dim.user_coord_address_day p
                           where dt = "%(statdate)s"

                           union all

                          select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,
                                 fuid,fplatform_uid,fc_country,fc_province,fc_city,fc_district,fc_street,flatitude,flongitude
                            from dim.user_coord_address
                           where dt = date_sub("%(statdate)s", 1)
                         ) t
                 ) tt
           where rown = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_coord_address(sys.argv[1:])
a()
