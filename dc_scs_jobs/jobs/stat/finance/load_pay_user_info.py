#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

"""
2016-07-04
取用户uid时，加入了登录表，因为下单表有很多平台uid没有上报
备注：很多业务在报游客数据的时候，给支付中心的平台uid为100，给支付中心的uid是一个，业务自己的uid却是多个
eg：
select fplatform_uid, fuid
from user_pay_info
where dt = '2016-07-03' and fbpid = '92D5C4E4A65EB7B9BEF9A085033D1E9A'
group by fplatform_uid, fuid

2016-08-29
经过沟通业务侧实际存在同一天一个平台uid对应多个uid；
也存在一个uid对应多个平台uid
"""

class load_pay_user_info(BaseStat):
    """ 生成付费用户中间表 """
    def create_tab(self):
        hql = """
        -- 主键 fbpid fuid fplatform_uid
        create table if not exists stage.user_pay_info
        (
            fbpid            string,
            fuid             bigint,
            fpay_at          date,
            fplatform_uid    string,
            ffirstpay_at     string
        )
        partitioned by (dt date);

        -- 主键 fbpid fuid fplatform_uid
        create table if not exists stage.pay_user_mid
        (
            fbpid               string,
            fuid                bigint,
            fplatform_uid       string,
            ffirst_pay_at       date,
            ffirst_pay_income   decimal(20,2)
        )
        partitioned by (dt date);"""
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """

        """ 要更新很多天的分区数据的时候，要做如下设置 """
        res = self.hq.exe_sql("""set hive.optimize.sort.dynamic.partition=true""")
        if res != 0:
            return res

        hql_list = []

        hql ="""
        drop table if exists stage.pay_user_stream_%(num_begin)s;
        create table if not exists stage.pay_user_stream_%(num_begin)s as
        select fbpid, fplatform_uid, max(fuid) fuid
        from
        (
            select fbpid, fuid, fplatform_uid
            from stage.user_order_stg
            where dt = '%(stat_date)s'
                and fuid != 0 and fplatform_uid is not null

            union all

            select fbpid, fuid, fplatform_uid
            from stage.user_signup_stg
            where dt = '%(stat_date)s'
                and fuid != 0 and fplatform_uid is not null

            union all

            select fbpid, fuid, fplatform_uid
            from stage.user_login_stg
            where dt = '%(stat_date)s'
                and fuid != 0 and fplatform_uid is not null
        ) tt
        group by fbpid, fplatform_uid
        """% self.hql_dict
        hql_list.append( hql )


        """
        先导入 uid > 0 的
        """
        hql = """
        insert overwrite table stage.user_pay_info
            partition (dt = "%(stat_date)s")
        select fbpid,
            fuid,
            '%(stat_date)s' fpay_at,
            fplatform_uid,
            null ffirstpay_at
        from stage.payment_stream_stg
        where dt = '%(stat_date)s'
            and fuid > 0
        group by fbpid, fuid, fplatform_uid
        """ % self.hql_dict
        hql_list.append( hql )

        """
        再导入uid = 0和为空的
        因为业务上报的原因，有些平台uid会关联到多个uid
        """
        hql = """
        insert into table stage.user_pay_info
            partition (dt = "%(stat_date)s")
        select a.fbpid,
            b.fuid,
            '%(stat_date)s' fpay_at,
            a.fplatform_uid,
            null ffirstpay_at
        from
        (
            select fbpid, fplatform_uid
            from stage.payment_stream_stg
            where dt = '%(stat_date)s'
                and coalesce(fuid,0) <= 0
            group by fbpid, fplatform_uid
        ) a
        left join
        stage.pay_user_stream_%(num_begin)s b
        on a.fbpid = b.fbpid and a.fplatform_uid = b.fplatform_uid
        where b.fuid is not null
        """ % self.hql_dict
        hql_list.append( hql )


        """
        找出当天首次付费的用户
        是否首次付费以 平台uid 是否首次出现为准
        不以 uid 首次出现为准是历史上很多 平台uid 没关联上 uid
        """
        hql = """
        insert overwrite table stage.pay_user_mid
        partition (dt = "%(stat_date)s")
        select a.fbpid,
            a.fuid,
            a.fplatform_uid,
            '%(stat_date)s' ffirst_pay_at,
            a.dip
        from
        (
            select a.fbpid, a.fplatform_uid,
                   case when max(a.fuid)>0 then max(a.fuid) else max(c.fuid) end  fuid,
                round(sum(fcoins_num * frate), 2) dip
            from stage.payment_stream_stg a
            left join stage.pay_user_stream_%(num_begin)s c
              on a.fbpid = c.fbpid and a.fplatform_uid = c.fplatform_uid
            where dt = '%(stat_date)s'
            group by a.fbpid, a.fplatform_uid
        ) a
        left join stage.pay_user_mid b
         on  a.fbpid = b.fbpid
         and a.fplatform_uid = b.fplatform_uid
         and b.dt < '%(stat_date)s'
        where b.fplatform_uid is null;
        drop table if exists stage.pay_user_stream_%(num_begin)s;
        """ % self.hql_dict
        hql_list.append( hql )


        """
        此表的历史数据有很多用的fuid为null，均为没关联上的用户
        所以关联当天的最新fuid去刷新历史中为null的uid
        """
        hql = """
        insert overwrite table stage.pay_user_mid
        partition (dt)
        select a.fbpid,
            nvl(a.fuid, b.fuid) fuid,
            a.fplatform_uid,
            a.ffirst_pay_at,
            a.ffirst_pay_income,
            a.dt
        from stage.pay_user_mid a
        left join
        (
            select fbpid, fplatform_uid,
                max(fuid) as fuid
            from stage.user_pay_info
            where dt = '%(stat_date)s'
            group by fbpid, fplatform_uid
        ) b
        on a.fbpid = b.fbpid and a.fplatform_uid = b.fplatform_uid
        """ % self.hql_dict
        hql_list.append( hql )

        # res = 0
        res = self.exe_hql_list(hql_list)
        return res


if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = load_pay_user_info(stat_date)
    a()
