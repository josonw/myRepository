#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_pay_product_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_product_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fversionfsk             bigint,
            fterminalfsk            bigint,
            fp_fsk                  varchar(50),
            fpayuser_cnt            bigint,
            fpayment_cnt            decimal(20,2),
            fpay_times              bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.pay_product_channel_fct
        (
            fdate           date,
            fgamefsk        bigint,
            fplatformfsk    bigint,
            fversionfsk     bigint,
            fterminalfsk    bigint,
            fp_fsk          string,
            fm_fsk          string,
            fpayuser_cnt    bigint,
            fpayment_cnt    decimal(20,2),
            fpay_times      bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        insert overwrite table analysis.pay_product_fct
        partition(dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
            m.fgamefsk,
            m.fplatformfsk,
            m.fversionfsk,
            m.fterminalfsk,

            case coalesce(a.fproduct_id,'0') when '0' then a.fp_id else a.fproduct_id end fp_fsk,
            count(distinct a.fplatform_uid) fpayuser_cnt,
            sum(a.fcoins_num * a.frate) fpayment_cnt,
            count(distinct a.forder_id) fpay_times
        from stage.payment_stream_stg a

        join analysis.bpid_platform_game_ver_map m
            on a.fbpid = m.fbpid
        where a.dt ='%(stat_date)s'
        group by
            m.fgamefsk,
            m.fplatformfsk,
            m.fversionfsk,
            m.fterminalfsk,
            case coalesce(a.fproduct_id,'0') when '0' then a.fp_id else a.fproduct_id end
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.pay_product_channel_fct partition
        (dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
            m.fgamefsk,
            m.fplatformfsk,
            m.fversionfsk,
            m.fterminalfsk,

            case coalesce(a.fproduct_id,'0') when '0' then a.fp_id else a.fproduct_id end fp_fsk,
            a.fm_id fm_fsk,
            count(distinct a.fplatform_uid) fpayuser_cnt,
            round( sum(a.fcoins_num * a.frate), 2 ) fpayment_cnt,
            count(a.forder_id) fpay_times
        from stage.payment_stream_stg a

        join analysis.bpid_platform_game_ver_map m
            on a.fbpid = m.fbpid
        where a.dt = '%(stat_date)s'
        group by m.fgamefsk,
            m.fplatformfsk,
            m.fversionfsk,
            m.fterminalfsk,
            case coalesce(a.fproduct_id,'0') when '0' then a.fp_id else a.fproduct_id end ,
            a.fm_id
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
    a = agg_pay_product_data(stat_date)
    a()
