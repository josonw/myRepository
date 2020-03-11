#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class update_user_attribute_dim(BaseStat):
    def create_tab(self):
        """ 用户属性维度表 """
        hql = """
        create table if not exists stage.user_attribute_dim
        (
        fbpid         varchar(50),
        fuid          bigint,
        fplatform_uid  varchar(50),
        fgrade          bigint, -- 注册表没有的字段
        flastlogin_at   string, -- 注册表没有的字段
        flastpay_at     string -- 注册表没有的字段
        )
        location '/dw/stage/user_attribute_dim'
        """
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res


    def stat(self):

        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )

        hql = """
        drop table if exists stage.user_attribute_dim_%(num_begin)s;

        create table stage.user_attribute_dim_%(num_begin)s as
        select '%(ld_daybegin)s' as fdate,
            fbpid, fuid,
            max(fplatform_uid)  fplatform_uid,
            max(flastlogin_at) flastlogin_at,
            max(flastpay_at) flastpay_at,
            max(flevel) flevel
        from
        (
            select fbpid, fuid,null fplatform_uid, '%(ld_daybegin)s' flastlogin_at, null flastpay_at, null flevel
            from stage.active_user_mid
            where dt = '%(ld_daybegin)s'

            union all

            select fbpid, fuid,fplatform_uid, null flastlogin_at, '%(ld_daybegin)s' flastpay_at, null flevel
            from stage.user_pay_info
            where dt = '%(ld_daybegin)s'

            union all

            -- 一个用户一天里等级有很多级，有很多垃圾数据，所以取最后一条
            -- 等级大于300的当垃圾处理
            select fbpid, fuid,null fplatform_uid, null flastlogin_at, null flastpay_at, flevel
            from
            (
                select fbpid,
                    fuid,
                    flevel,
                    row_number() over(partition by fbpid, fuid order by uus.fgrade_at desc, flevel desc) as rowcn
                from stage.user_grade_stg uus
                where uus.dt = '%(ld_daybegin)s'
                and nvl(flevel,9999) < 300
            ) a
            where rowcn = 1

            union all

            -- 还要从登陆的级别里更新，最后根据升级和登陆取最大值
            select fbpid, fuid,null fplatform_uid, null flastlogin_at, null flastpay_at, flevel
            from
            (
                select fbpid,
                    fuid,
                    flevel,
                    row_number() over(partition by fbpid,fuid order by flogin_at desc ) rowcn
                    from stage.user_login_stg
                    where dt='%(ld_daybegin)s'
                and nvl(flevel,9999) < 300 and flevel > 0
            ) a
            where rowcn = 1

        ) b
        group by fbpid, fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:return res


        hql ="""
        insert overwrite table stage.user_attribute_dim
        select
            fbpid,
            fuid,
            max(fplatform_uid) fplatform_uid,
            max(fgrade) fgrade,
            max(flastlogin_at) flastlogin_at,
            max(flastpay_at) flastpay_at
        from (
              select fbpid, fuid, fplatform_uid, fgrade, flastlogin_at, flastpay_at
                from stage.user_attribute_dim

              union all

              select fbpid, fuid, fplatform_uid, flevel fgrade, flastlogin_at, flastpay_at
              from stage.user_attribute_dim_%(num_begin)s
              ) a

        group by a.fbpid,a.fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        res = self.hq.exe_sql("""drop table if exists stage.user_attribute_dim_%(num_begin)s;"""% query)

        return res



#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = update_user_attribute_dim(statDate)
a()
