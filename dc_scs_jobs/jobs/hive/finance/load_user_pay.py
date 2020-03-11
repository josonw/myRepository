#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const



class load_user_pay(BaseStat):
    """
    生成付费用户中间表
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_pay
        (
            fbpid               string,
            fgame_id            bigint,              --子游戏ID
            fchannel_code       bigint,              --渠道ID
            fuid                bigint,
            fplatform_uid       string,
            ffirst_pay_income   decimal(20,2),
            ffirstpay_at        string,              --首次付费时间
            ffirst_pay_at       date,                --首次付费日期
            fpay_cnt            INT         COMMENT '当日付费次数', --20160912增加
            ffirst_first_income decimal(20,2)   COMMENT '首付用户首单金额' --20160912增加
        )
        partitioned by (dt date);"""
        result = self.exe_hql(hql)
        if result != 0:return result


    def stat(self):
        """ 重要部分，统计内容 """
        query = {'stat_date':self.stat_date,
                 'null_int_report':sql_const.NULL_INT_REPORT}
        query.update(PublicFunc.date_define(self.stat_date))

        hql_list = []

        hql = """
        -- 找出当天首次付费的用户
        insert overwrite table dim.user_pay
        partition (dt = "%(stat_date)s")
        select
            a.fbpid,
            fgame_id,
            fchannel_code,
            fuid,
            a.fplatform_uid,
            ffirst_pay_income,
            ffirstpay_at,
            "%(stat_date)s" ffirst_pay_at,
            fpay_cnt, --当日付费次数
            ffirst_first_income --首付用户首单金额
        from (select fbpid, fgame_id, fchannel_code, fuid, fplatform_uid, ffirst_pay_income,ffirstpay_at, fpay_cnt
                from (select fbpid,
                             fgame_id,
                             fchannel_code,
                             fuid,
                             fplatform_uid,
                             ftotal_usd_amt  ffirst_pay_income,
                             ffirstpay_at,
                             fpay_cnt,
                             row_number() over(partition by fbpid,fplatform_uid order by ffirstpay_at asc) row_num
                        from dim.user_pay_day
                       where dt = '%(stat_date)s') a
               where row_num=1
             ) a
        left join ( select fbpid, fplatform_uid
                      from dim.user_pay
                     where dt < '%(stat_date)s' and fplatform_uid is not null
                     group by fbpid, fplatform_uid
             ) b
          on a.fbpid = b.fbpid and a.fplatform_uid = b.fplatform_uid
        left join (select fbpid, fplatform_uid, ffirst_first_income
                     from (select fbpid, fplatform_uid,round(fcoins_num * frate,6) ffirst_first_income,
                                  row_number() over(partition by fbpid,fplatform_uid order by fsucc_time asc) row_num --首付用户首单金额
                             from stage.payment_stream_stg
                            where dt='%(stat_date)s'
                          ) t where row_num = 1
                   ) c
               on a.fbpid = c.fbpid and a.fplatform_uid = c.fplatform_uid
       where b.fplatform_uid is null
        """ % query
        hql_list.append( hql )


        hql = """
        drop table if exists work.user_stream_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

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
    a = load_user_pay(stat_date)
    a()
