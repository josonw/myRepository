#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gamecoin_detail_data(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.pay_game_coin_finace_fct(
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
          fpaidnum bigint,
          fpaidusernum bigint
        )
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
            "ld_begin": self.stat_date,
            "num_date": self.stat_date.replace("-", "")
        }

        hql = """
          use stage;

          drop table if exists gamecoin_detail_data_tmp_%(num_date)s;

          create table gamecoin_detail_data_tmp_%(num_date)s as
          select fbpid, fuid, act_id, act_type, sum(abs(act_num)) act_num, count(fuid) fcnt
            from pb_gamecoins_stream_stg
           where dt = '%(ld_begin)s'
             and act_type in (1, 2)
        group by fbpid, fuid, act_id, act_type;

          insert overwrite table analysis.pay_game_coin_finace_fct
            partition(dt='%(ld_begin)s')
            select '%(ld_begin)s' fdate,
                   b.fgamefsk,
                   b.fplatformfsk,
                   b.fversion_old fversionfsk,
                   b.fterminalfsk,
                   'GAMECOIN' fcointype,
                   case g.act_type when 1 then 'IN'when 2 then 'OUT' end fdirection,
                   g.act_id ftype,
                   sum(abs(g.act_num)) fnum,
                   count(distinct g.fuid) fusernum,
                   count(distinct case when pay_user = 1 then g.fuid end) fpayusernum,
                   sum(case when pay_user = 1 then abs(g.act_num) end) fpaynum,
                   sum(fcnt) fcnt,
                   sum(case when paid_user = 1 then abs(g.act_num) end) fpaidnum,
                   count(distinct case when paid_user = 1 then g.fuid end) fpaidusernum
              from stage.gamecoin_detail_data_tmp_%(num_date)s g
         left join (
                    select fbpid, fuid, max(paid_user) paid_user, max(pay_user) pay_user
                      from (
                            select fbpid, fuid, 1 paid_user, 0 pay_user
                              from stage.active_paid_user_mid
                             where dt = "%(ld_begin)s"
                              union all
                            select fbpid, fuid, 0 paid_user, 1 pay_user
                              from stage.user_pay_info
                             where dt = "%(ld_begin)s"
                           ) t
                     group by fbpid, fuid
                   ) t
                on g.fbpid = t.fbpid
               and g.fuid = t.fuid
              join dim.bpid_map b
                on g.fbpid = b.fbpid
             group by b.fgamefsk,
                      b.fplatformfsk,
                      b.fversion_old,
                      b.fterminalfsk,
                      g.act_type,
                      g.act_id;
              """ % args_dic

        hql += """
        drop table if exists gamecoin_detail_data_tmp_%(num_date)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    a = agg_gamecoin_detail_data()
    a()
