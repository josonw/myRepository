#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经注释关闭
class agg_pay_country_ch_final_fct(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_country_channel_final_fct
        (
          fdate      date,
          fsid       decimal(20),
          fappid     decimal(20),
          fpmode     decimal(20),
          fchannel   varchar(128),
          fsitename  varchar(128),
          fappname   varchar(128),
          fpmodename varchar(128),
          fcompanyid decimal(20),
          fuser      decimal(20),
          fincome    decimal(20,6),
          fpamount   decimal(20,6),
          fpuser     decimal(20)
        )
        partitioned by (dt date) """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 0

        #加上当天的分区
        hql = """
        use analysis;
        alter table pay_country_channel_final_fct add if not exists partition (dt='%(ld_begin)s')
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        add jar hdfs://192.168.0.92:8020/dw/udf/nexr-hive-udf-0.4.jar;
        CREATE TEMPORARY FUNCTION trunc AS 'com.nexr.platform.hive.udf.UDFTrunc';

        insert overwrite table analysis.pay_country_channel_final_fct partition(dt)
        select forder_date as fdate,
            fsid, fappid, fpmode, fchannel,
            max(b.sitename) as fsitename, max(b.appname) as fappname,
            max(c.pmodename) as fpmodename, max(c.companyid) as fcompanyid,
            sum(fsucc_user) as fuser,
            sum(fsucc_income) as fincome,
            sum(fsucc_pamount) as fpamount,
            sum(fsucc_puser) as fpuser,
            forder_date as dt
        from
        (
            select trunc(fdate,'DD') as forder_date, sid as fsid, appid as fappid, pmode as fpmode,
                case when sid=8 and pmode=6 then '-1' else nvl(pbankno,'-1') end as fchannel,

                max(fbpid) as fbpid,

                -- fuid
                -- 成功人数、收入
                count(distinct case when pstatus = 2 then fuid else null end) as fsucc_user,
                sum(case when pstatus = 2 then fusd else 0 end) as fsucc_income,
                sum(case when pstatus = 2 then fcoins_num else 0 end) as fsucc_pamount,
                -- 手工补单人数、收入
                count(distinct case when pstatus = 2 and  ext_9 = '2' then fuid else null end) as fsucc_user_add,
                sum(case when pstatus = 2 and  ext_9 = '2' then fusd else 0 end) as fsucc_income_add,

                -- 退单人数、收入
                count(distinct case when pstatus = 3 and fdesc like '%%|2|%%' then fuid else null end) as fquit_user,
                sum(case when pstatus = 3 and fdesc like '%%|2|%%' then fusd else 0 end) as fquit_income,
                sum(case when pstatus = 3 and fdesc like '%%|2|%%' then fcoins_num else 0 end) as fquit_pamount,
                -- 手工退单人数、收入；欺诈人数
                count(distinct case when pstatus = 3 and fdesc like '%%|2|%%' and  ext_9 = '3' then fuid else null end) as fquit_user_add,
                sum(case when pstatus = 3 and fdesc like '%%|2|%%' and ext_9 = '3' then fusd else 0 end) as fquit_income_add,
                count(distinct case when pstatus = 5 then fuid else null end) as fquit_user_cheat,

                -- fplatform_uid
                -- 总人数；手工补单人数
                count(distinct case when pstatus = 2 then fplatform_uid else null end) as fsucc_puser,
                count(distinct case when pstatus = 2 and ext_9 = '2' then fplatform_uid else null end) as fsucc_puser_add,

                -- 退单人数；手工退单人数；欺诈人数
                count(distinct case when pstatus = 3 and fdesc like '%%|2|%%' then fplatform_uid else null end) as fquit_puser,
                count(distinct case when pstatus = 3 and fdesc like '%%|2|%%' and  ext_9 = '3' then fplatform_uid else null end) as fquit_puser_add,
                count(distinct case when pstatus = 5 then fplatform_uid else null end) as fquit_puser_cheat
            from stage.payment_stream_mid
            where dt >= date_add('%(ld_begin)s', -150) and dt < '%(ld_end)s'
              and fdate >= date_add('%(ld_begin)s', -150)
            group by trunc(fdate,'DD'), sid, appid, pmode,
                case when sid=8 and pmode=6 then '-1' else nvl(pbankno,'-1') end
        ) a
        left join
        (
            select sid, appid, '' sitename, max(appname) as appname
            from analysis.paycenter_apps_dim
            group by sid, appid
        ) b
        on a.fsid = b.sid and a.fappid = b.appid
        left join
        (
            select sid, pmode, max(pmodename) as pmodename, max(companyid) as companyid
            from analysis.paycenter_chanel_dim ta
            group by sid, pmode
        ) c
        on a.fsid = c.sid and a.fpmode = c.pmode
        group by forder_date, fsid, fappid, fpmode, fchannel
        """ % self.hql_dict
        hql_list.append( hql )


        result = self.exe_hql_list(hql_list)
        return result



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
    a = agg_pay_country_ch_final_fct(stat_date)
    a()
