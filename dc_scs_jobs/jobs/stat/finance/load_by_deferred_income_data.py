#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_by_deferred_income_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.by_deferred_income_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fcoins_num              decimal(38,2),
            fcoins_usb              decimal(38,2),
            frate_rmb               decimal(20,7),
            fgamecoin_in            bigint,
            fgamecoin_out           bigint,
            fincome                 decimal(38,2),
            fgamecoin_rate          decimal(20,7),
            fdefer_income           decimal(38,2)
        )
        partitioned by (dt date);        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        alter table by_deferred_income_fct drop partition(dt = "%(ld_1month_ago_begin)s")
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
          insert into table analysis.by_deferred_income_fct partition
            (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
            fgamefsk,
            fplatformfsk,
            null fcoins_num,
            null fcoins_usb,
            null frate_rmb,
            nvl(sum(case when fdirection = 'IN' then fusernum end), 0) fgamecoin_in,
            nvl(sum(case when fdirection = 'OUT' then fusernum end), 0) fgamecoin_out,
            null fincome,
            null fgamecoin_rate,
            null fdefer_income
         from analysis.pay_game_coin_finace_fct
        where dt >= '%(ld_1month_ago_begin)s'
          and dt < '%(ld_month_begin)s'
        group by fgamefsk, fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
          insert into table analysis.by_deferred_income_fct partition
            (dt = "%(ld_1month_ago_begin)s")
        select fdate,
                fgamefsk,
                fplatformfsk,
                sum(paynum) fcoins_num,
                sum(usdpaynum) fcoins_usb,
                null frate_rmb,
                null fgamecoin_in,
                null fgamecoin_out,
                null fincome,
                null fgamecoin_rate,
                null fdefer_income
             from (select '%(ld_1month_ago_begin)s' fdate,
                          fbpid,
                          sum(fcoins_num) paynum,
                          sum(round(fcoins_num * frate, 2)) usdpaynum
                     from stage.payment_stream_stg
                    where dt >= '%(ld_1month_ago_begin)s'
                      and dt < '%(ld_month_begin)s'
                    group by fbpid) a
             join dim.bpid_map b
               on a.fbpid = b.fbpid
            group by fdate, fgamefsk, fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
          insert into table analysis.by_deferred_income_fct partition
                (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
                    fgamefsk,
                    fplatformfsk,
                    null fcoins_num,
                    null fcoins_usb,
                    null frate_rmb,
                    null fgamecoin_in,
                    null fgamecoin_out,
                    null fincome,
                  case when (qcjy + fgamecoin_in - fgamecoin_out) > 0
                  then (case when  (qcjy - fgamecoin_out) > 0
                             then (qcjy - fgamecoin_out)
                             else 0
                             end
                        * fgamecoin_rate) / (qcjy + fgamecoin_in - fgamecoin_out  )
                  else 0 end fgamecoin_rate,
                null fdefer_income
             from (select qc.fgamefsk,
                          qc.fplatformfsk,
                          nvl(qcjy, 0) qcjy,
                          fgamecoin_in,
                          fgamecoin_out,
                          case when fgamecoin_rate > 0 then fgamecoin_rate  else 1 end fgamecoin_rate
                     from (select fgamefsk,
                                  fplatformfsk,
                                  fgamecoin_in,
                                  fgamecoin_out
                             from analysis.by_deferred_income_fct
                            where dt = '%(ld_1month_ago_begin)s') qc
                     left join (select fgamefsk, fplatformfsk,
                                      case when (sum(fgamecoin_in) - sum(fgamecoin_out)) > 0
                                           then (sum(fgamecoin_in) - sum(fgamecoin_out))
                                           else 0 end qcjy
                                 from analysis.by_deferred_income_fct
                                where dt < '%(ld_1month_ago_begin)s'
                                group by fgamefsk, fplatformfsk) bq
                       on qc.fgamefsk = bq.fgamefsk
                      and qc.fplatformfsk = bq.fplatformfsk
                     left join (select fgamefsk, fplatformfsk, fgamecoin_rate
                                 from analysis.by_deferred_income_fct
                                where dt = '%(ld_2month_ago_begin)s') sq
                       on qc.fgamefsk = sq.fgamefsk
                      and qc.fplatformfsk = sq.fplatformfsk) t
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.by_deferred_income_fct partition
            (dt = "%(ld_1month_ago_begin)s")
        select fdate,
            fgamefsk,
            fplatformfsk,
            max(fcoins_num)             fcoins_num,
            max(fcoins_usb)             fcoins_usb,
            max(frate_rmb)              frate_rmb,
            max(fgamecoin_in)           fgamecoin_in,
            max(fgamecoin_out)          fgamecoin_out,
            max(fincome)                fincome,
            max(fgamecoin_rate)         fgamecoin_rate,
            max(fdefer_income)          fdefer_income
        from analysis.by_deferred_income_fct
        where dt = "%(ld_1month_ago_begin)s"
        group by fdate,
                fgamefsk,
                fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        # 只有月初日期才会执行
        if self.hql_dict.get('stat_date', '').endswith('-01'):
            result = self.exe_hql_list(hql_list)
        else:
            result = ''
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
    a = load_by_deferred_income_data(stat_date)
    a()
