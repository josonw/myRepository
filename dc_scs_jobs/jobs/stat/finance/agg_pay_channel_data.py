#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_pay_channel_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_channel_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fm_fsk                      string,
            fpayuser_cnt                bigint,          --成功付费人数
            fpayment_cnt                decimal(20,2),   --成功付费金额
            fpay_times                  bigint,          --成功下单数
            fnewpayuser_cnt             bigint,          --成功首次付费用户数
            fnewpayment_cnt             decimal(20,2),   --成功首次付费用户付费金额
            fnewpay_times               bigint,          --成功首次付费用户下单次数
            fordercnt                   bigint,            --下单数
            fpay_unum                   bigint,            --付费人数
            fnewpay_ucnt                bigint,            --首次付费用户下单次数
            fnewpay_unum                bigint             --首次付费用户数
        )
        partitioned by (dt date)
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        alter table pay_channel_fct drop partition(dt = "%(stat_date)s");
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """
        insert overwrite table analysis.pay_channel_fct partition(dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
              d.fgamefsk,
              d.fplatformfsk,
              d.fversionfsk,
              d.fterminalfsk,
              coalesce(c.fm_fsk, cast(a.fm_id as bigint) ) fm_fsk,
              count(DISTINCT case when a.pstatus=2 then a.fplatform_uid else null end) fpayuser_cnt,
              sum(case when a.pstatus=2 then a.fcoins_num * a.frate else 0 end) fpayment_cnt,
              count(DISTINCT case when a.pstatus=2 then a.forder_id else null end) fpay_times,
              count(DISTINCT case when a.pstatus=2 and b.fplatform_uid is not null then a.fplatform_uid else null end) fnewpayuser_cnt,
              sum(case when a.pstatus=2 and b.fplatform_uid is not null then a.fcoins_num * a.frate else 0 end) fnewpayment_cnt,
              count(DISTINCT case when a.pstatus=2 and b.fplatform_uid is not null then a.forder_id else null end) fnewpay_times,
              count(a.forder_id) fordercnt,
              count(DISTINCT a.fplatform_uid ) fpay_unum,
              count(DISTINCT case when b.fplatform_uid is not null then a.forder_id else null end) fnewpay_ucnt,
              count(DISTINCT case when b.fplatform_uid is not null then a.fplatform_uid else null end) fnewpay_unum
         from (
               select a.fbpid,
                        a.forder_id,  a.fplatform_uid, a.fcoins_num, a.frate ,a.fm_id,a.fm_name, a.pstatus
                    FROM stage.payment_stream_all_stg a
                    WHERE a.dt = '%(stat_date)s'
                group by a.fbpid, a.forder_id, a.fplatform_uid, a.fcoins_num, a.frate ,a.fm_id,a.fm_name, a.pstatus
              ) a
         left join stage.pay_user_mid b
           on a.fbpid = b.fbpid
          and a.fplatform_uid = b.fplatform_uid
          and b.dt='%(stat_date)s'
         left join analysis.payment_channel_dim c
           on a.fm_id = c.fm_id
         join analysis.bpid_platform_game_ver_map d
           on a.fbpid = d.fbpid
        group by d.fgamefsk,
                 d.fplatformfsk,
                 d.fversionfsk,
                 d.fterminalfsk,
                 coalesce(c.fm_fsk, cast(a.fm_id as bigint) )
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
    a = agg_pay_channel_data(stat_date)
    a()
