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


class load_user_version_info(BaseStatModel):
    def create_tab(self):
        hql = """--当天出现过得版本，一个用户不止一条
        create table if not exists stage_dfqp.user_version_info_day (
               fdate               string,
               fbpid               varchar(50),
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuid                bigint,
               fversion_info       string      comment '版本'
               )comment '用户版本信息'
               partitioned by(dt date)
        location '/dw/stage_dfqp/user_version_info_day';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        create table if not exists stage_dfqp.user_version_info (
               fdate               string,
               fbpid               varchar(50),
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuid                bigint,
               fversion_max        string      comment '历史最高版本'
               )comment '用户最高版本信息'
               partitioned by(dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set mapreduce.map.memory.mb=2048;set mapreduce.map.java.opts=-XX:-UseGCOverheadLimit -Xmx1700m;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--版本_当日
        insert overwrite table stage_dfqp.user_version_info_day
        partition(dt="%(statdate)s")
          select distinct '%(statdate)s' fdate
                 ,t1.fbpid
                 ,tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,fuid
                 ,coalesce(t1.fversion_info, '0') fversion_info
            from dim.user_login_additional t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
             and tt.fgamefsk = 4132314431
           where dt= '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """ -- 版本_全量_最高版本
        insert overwrite table stage_dfqp.user_version_info
        partition(dt="%(statdate)s")
         select tt.fdate
                ,tt.fbpid
                ,tt.fgamefsk
                ,tt.fplatformfsk
                ,tt.fhallfsk
                ,tt.fterminaltypefsk
                ,tt.fversionfsk
                ,tt.fuid
                ,tt.fversion_max
           from (select fdate
                        ,fbpid
                        ,fgamefsk
                        ,fplatformfsk
                        ,fhallfsk
                        ,fterminaltypefsk
                        ,fversionfsk
                        ,fuid
                        ,fversion_max
                        ,row_number() over(partition by t.fbpid, fuid order by row_num desc) row_num
                   from (select fdate
                                ,fbpid
                                ,fgamefsk
                                ,fplatformfsk
                                ,fhallfsk
                                ,fterminaltypefsk
                                ,fversionfsk
                                ,fuid
                                ,fversion_max
                                ,cast (coalesce(split(fversion_max,'\\\\.')[3],split(fversion_max,'\\\\.')[2],0) as bigint) row_num
                           from stage_dfqp.user_version_info t1
                          where dt = date_sub("%(statdate)s", 1)

                          union all

                         select fdate
                                ,fbpid
                                ,fgamefsk
                                ,fplatformfsk
                                ,fhallfsk
                                ,fterminaltypefsk
                                ,fversionfsk
                                ,fuid
                                ,fversion_info fversion_max
                                ,cast (coalesce(split(fversion_info,'\\\\.')[3],split(fversion_info,'\\\\.')[2],0) as bigint) row_num
                           from stage_dfqp.user_version_info_day t1
                          where t1.dt = "%(statdate)s"
                        ) t
                ) tt
          where row_num = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_version_info(sys.argv[1:])
a()
