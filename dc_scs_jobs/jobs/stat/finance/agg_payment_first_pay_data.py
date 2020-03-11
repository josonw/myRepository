#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_first_pay_data(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求, 90天  --没计算出来
        create table if not exists analysis.user_reg_pay_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fpaydate                    date,
            fpayusernum                 bigint,
            fincome                     decimal(38,2),
            ffirstpayusernum            bigint,
            ffirstincome                decimal(38,2)
        )
        partitioned by (dt date);

        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1
        # 特殊同步要求, 90天

        #加上当天的分区
        hql = """
        use analysis;
        alter table user_reg_pay_fct add if not exists partition (dt='%(ld_begin)s')
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_reg_pay_fct partition(dt)
        select fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            cast ('%(ld_begin)s' as date) fpaydate,
            count(distinct payuid) fpayusernum,
            round(sum(dip),2) fincome,
            count(distinct fpayuid) ffirstpayusernum,
            round(sum(case when fpayuid is not null then dip end),2) ffirstincome,
            fdate dt
        from
        (
            select ud.dt fdate,
            m.fplatformfsk,
            m.fgamefsk,
            m.fversion_old fversionfsk,
            m.fterminalfsk,
            od1.fplatform_uid payuid,
            case when od2.dt= '%(ld_begin)s' then od2.fplatform_uid end fpayuid,
            sum(fcoins_num * frate) dip
        from stage.payment_stream_stg od1
        left join stage.pay_user_mid od2
          on od1.fplatform_uid = od2.fplatform_uid
         and od1.fbpid = od2.fbpid
        join stage.user_dim ud
         on od2.fbpid = ud.fbpid
        and od2.fuid = ud.fuid
        and ud.dt >= '%(ld_90dayago)s'
        and ud.dt <  '%(ld_end)s'
        join dim.bpid_map m
        on od1.fbpid = m.fbpid
        where od1.dt = '%(ld_begin)s'
        group by ud.dt,
            od2.dt,
            m.fplatformfsk,
            m.fgamefsk,
            m.fversion_old,
            m.fterminalfsk,
            od1.fplatform_uid,
            od2.fplatform_uid
        ) a
        group by fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk

        union all

        select fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fpaydate,
                fpayusernum,
                fincome,
                ffirstpayusernum,
                ffirstincome,
                fdate dt
         from analysis.user_reg_pay_fct
         where dt >= '%(ld_90dayago)s'
           and dt <  '%(ld_end)s'
           and fpaydate != '%(ld_begin)s'

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_reg_pay_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fpaydate,
        fpayusernum,
        fincome,
        ffirstpayusernum,
        ffirstincome
        from analysis.user_reg_pay_fct
        where dt >= '%(ld_90dayago)s'
         and dt <  '%(ld_end)s'

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
    a = agg_payment_first_pay_data(stat_date)
    a()
