#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_bycoin_stream_day(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.user_bycoin_stream_fct(
          fdate date,
          fgamefsk bigint,
          fplatformfsk bigint,
          fversionfsk bigint,
          fterminalfsk bigint,
          fcointype varchar(50),
          fdirection varchar(50),
          ftype varchar(50),
          fnum bigint,
          fusernum int,
          fpayusernum int,
          fpaynum bigint,
          fcnt bigint,
          fpaycnt bigint)
        partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": self.stat_date
        }

        hql = """
              use stage;

          insert overwrite table analysis.user_bycoin_stream_fct
            partition(dt='%(ld_begin)s')
            select '%(ld_begin)s' fdate,
                   b.fgamefsk,
                   b.fplatformfsk,
                   b.fversion_old fversionfsk,
                   b.fterminalfsk,
                   'BYCOIN' fcointype,
                   case
                     when p.fact_type=1 then 'IN'
                     when p.fact_type=2 then 'OUT'
                   end fdirection,
                   p.fact_id ftype,
                   sum(abs(p.fact_num)) fnum,
                   count(distinct p.fuid) fusernum,
                   count(distinct case
                           when u.fbpid is not null then
                            p.fuid
                         end) fpayusernum,
                   sum(case
                         when u.fbpid is not null then
                          abs(p.fact_num)
                       end) fpaynum,
                   count(1) fcnt,
                   count(case
                           when u.fbpid is not null then
                           1
                         end) fpaycnt
              from stage.pb_bycoins_stream_stg p
              left join stage.user_pay_info u
                on p.fbpid = u.fbpid
               and p.fuid = u.fuid
               and u.dt = '%(ld_begin)s'
              join dim.bpid_map b
                on b.fbpid = p.fbpid
             where p.dt = '%(ld_begin)s'
               and p.fact_type in (1,2)
             group by b.fgamefsk,
                      b.fplatformfsk,
                      b.fversion_old,
                      b.fterminalfsk,
                      case
                       when p.fact_type=1 then 'IN'
                       when p.fact_type=2 then 'OUT'
                     end,
                      p.fact_id;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = agg_user_bycoin_stream_day()
    a()
