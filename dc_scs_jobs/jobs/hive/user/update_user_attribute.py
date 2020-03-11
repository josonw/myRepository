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

class update_user_attribute(BaseStatModel):
    def create_tab(self):
        hql = """--全量用户属性维度表
        create table if not exists dim.user_attribute
              (
                fbpid           varchar(50),
                fuid            bigint,
                fplatform_uid   varchar(50),
                fgrade          bigint, -- 注册表没有的字段
                flastlogin_at   string, -- 注册表没有的字段
                flastpay_at     string -- 注册表没有的字段
              )comment '全量用户属性维度表'
        location '/dw/dim/user_attribute'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set mapreduce.map.memory.mb=2048;set mapreduce.map.java.opts=-Xmx1700m;""")
        if res != 0:
            return res

        hql = """--
        drop table if exists work.user_attribute_tmp_%(statdatenum)s;
        create table work.user_attribute_tmp_%(statdatenum)s as
        select '%(statdate)s' fdate,
               fbpid,
               fuid,
               max(fplatform_uid) fplatform_uid,
               max(flastlogin_at) flastlogin_at,
               max(flastpay_at) flastpay_at,
               max(flevel) flevel
          from (select fbpid,
                       fuid,
                       null fplatform_uid,
                       '%(statdate)s' flastlogin_at,
                       null flastpay_at,
                       null flevel
                  from dim.user_act
                 where dt = '%(statdate)s'
                 union all
                select fbpid,
                       fuid,
                       fplatform_uid,
                       null flastlogin_at,
                       '%(statdate)s' flastpay_at,
                       null flevel
                  from dim.user_pay_day
                 where dt = '%(statdate)s'
                 union all

                     -- 一个用户一天里等级有很多级，有很多垃圾数据，所以取最后一条
                     -- 等级大于300的当垃圾处理
                 select fbpid, fuid,
                        null fplatform_uid, null flastlogin_at, null flastpay_at, flevel
                   from (select fbpid,
                                fuid,
                                flevel,
                                row_number() over(partition by fbpid, fuid order by uus.fgrade_at desc, flevel desc) as rowcn
                           from stage.user_grade_stg uus
                          where uus.dt = '%(statdate)s'
                            and nvl(flevel,9999) < 300
                        ) a
                  where rowcn = 1
                  union all
                     -- 还要从登陆的级别里更新，最后根据升级和登陆取最大值
                 select fbpid, fuid,
                        null fplatform_uid, null flastlogin_at, null flastpay_at, flevel
                   from (select fbpid,
                                fuid,
                                flevel,
                                row_number() over(partition by fbpid,fuid order by flogin_at desc ) rowcn
                           from dim.user_login_additional
                          where dt='%(statdate)s'
                            and nvl(flevel,9999) < 300 and flevel > 0
                        ) a
                  where rowcn = 1
               ) b
         group by fbpid, fuid
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--汇总
        insert overwrite table dim.user_attribute
        select fbpid,
               fuid,
               max(fplatform_uid) fplatform_uid,
               max(fgrade) fgrade,
               max(flastlogin_at) flastlogin_at,
               max(flastpay_at) flastpay_at
          from (select fbpid, fuid,
                       fplatform_uid, fgrade, flastlogin_at, flastpay_at
                  from dim.user_attribute
                 union all
                select fbpid, fuid,fplatform_uid, flevel fgrade, flastlogin_at, flastpay_at
                  from work.user_attribute_tmp_%(statdatenum)s
               ) a
         group by a.fbpid,a.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_attribute_tmp_%(statdatenum)s;

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = update_user_attribute(sys.argv[1:])
a()
