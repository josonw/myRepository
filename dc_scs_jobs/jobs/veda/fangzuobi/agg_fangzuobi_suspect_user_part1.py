# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_fangzuobi_suspect_user_part1(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
insert overwrite table veda.user_basic_day partition(dt='%(statdate)s')
select '地方棋牌'
       ,x.fuid
       ,sum(x.flogin_cnt) flogin_cnt
       ,sum(nvl(a.ftotal_usd_amt, 0)) ftotal_usd_amt
       ,sum(nvl(a.fpay_cnt, 0)) fpay_cnt
       ,sum(nvl(b.fplay_time, 0)) fplay_time
       ,sum(nvl(b.fparty_num, 0)) fparty_num
       ,sum(nvl(b.fcharge, 0)) fcharge
       ,sum(nvl(b.fwin_amt, 0)) fwin_amt
       ,sum(nvl(b.flose_amt, 0)) flose_amt
       ,sum(nvl(b.fwin_party_num, 0)) fwin_party_num
       ,sum(nvl(b.flose_party_num, 0)) flose_party_num
       ,sum(nvl(i.fsubgame_played_cnt, 0)) fsubgame_played_cnt
       ,sum(nvl(c.fmatch_signup_cnt, 0)) fmatch_signup_cnt
       ,sum(nvl(d.fmatch_join_cnt, 0)) fmatch_join_cnt
       ,sum(nvl(e.fmatch_win_cnt, 0)) fmatch_win_cnt
       ,sum(nvl(f.fgamecoins_num, 0)) fgamecoins_num
       ,sum(nvl(g.fcurrencies_num, 0)) fcurrencies_num
       ,sum(nvl(h.frupt_cnt, 0)) frupt_cnt
       ,0
  from (select x1.fbpid
               ,x1.fuid
               ,x1.flogin_cnt
          from dim.user_act_main x1
          left join dim.bpid_map x2
            on x1.fbpid = x2.fbpid
         where x2.fgamename = '地方棋牌'
           and x1.dt = '%(statdate)s'
       ) x
left join (select fbpid, fuid, sum(ftotal_usd_amt) ftotal_usd_amt, sum(fpay_cnt) fpay_cnt from dim.user_pay_day where dt = '%(statdate)s' group by fbpid, fuid) a
  on x.fbpid = a.fbpid and x.fuid = a.fuid
left join (select m.fbpid, m.fuid, sum(m.fplay_time) fplay_time, sum(m.fparty_num) fparty_num, sum(m.fcharge) fcharge, sum(m.fwin_amt) fwin_amt, sum(m.flose_amt) flose_amt, sum(m.fwin_party_num) fwin_party_num, sum(flose_party_num) flose_party_num
             from dim.user_gameparty m
             left join dim.bpid_map n
               on m.fbpid = n.fbpid
            where m.dt = '%(statdate)s' and n.fgamename = '地方棋牌' group by m.fbpid, m.fuid) b
  on x.fbpid = b.fbpid and x.fuid = b.fuid
left join (select o.fbpid, o.fuid, count(*) fmatch_signup_cnt from stage.join_gameparty_stg o
        left join dim.bpid_map p on o.fbpid = p.fbpid where o.dt = '%(statdate)s' and p.fgamename = '地方棋牌' group by o.fbpid, o.fuid) c
  on x.fbpid = c.fbpid and x.fuid = c.fuid
left join (select s.fbpid, s.fuid, count(*) fmatch_join_cnt from (select distinct q.fbpid, q.fuid, q.fmatch_id  from stage.user_gameparty_stg q
        left join dim.bpid_map r on q.fbpid = r.fbpid where q.dt = '%(statdate)s' and r.fgamename = '地方棋牌' and nvl(q.fmatch_id, '0') <> '0') s group by s.fbpid, s.fuid) d
  on x.fbpid = d.fbpid and x.fuid = d.fuid
left join (select u.fbpid, u.fuid, sum(u.fio_type) fmatch_win_cnt from stage.match_economy_stg u
        left join dim.bpid_map v on u.fbpid = v.fbpid where u.dt = '%(statdate)s' and v.fgamename = '地方棋牌' group by u.fbpid, u.fuid) e
  on x.fbpid = e.fbpid and x.fuid = e.fuid
left join (select f1.fbpid, f1.fuid, sum(f1.fgamecoins) fgamecoins_num from dim.user_gamecoin_balance_day f1
        left join dim.bpid_map f2 on f1.fbpid = f2.fbpid where f1.dt = '%(statdate)s' and f2.fgamename = '地方棋牌' group by f1.fbpid, f1.fuid) f
  on x.fbpid = f.fbpid and x.fuid = f.fuid
left join (select g1.fbpid, g1.fuid, sum(g1.fcurrencies_num) fcurrencies_num from dim.user_currencies_balance_day g1
        left join dim.bpid_map g2 on g1.fbpid = g2.fbpid where g1.dt = '%(statdate)s' and g1.fcurrencies_type = 11 and g2.fgamename = '地方棋牌' group by g1.fbpid, g1.fuid) g
  on x.fbpid = g.fbpid and x.fuid = g.fuid
left join (select h1.fbpid, h1.fuid, sum(h1.frupt_cnt) frupt_cnt from dim.user_bankrupt_relieve h1
        left join dim.bpid_map h2 on h1.fbpid = h2.fbpid where h1.dt = '%(statdate)s' and h2.fgamename = '地方棋牌' group by h1.fbpid, h1.fuid) h
  on x.fbpid = h.fbpid and x.fuid = h.fuid
left join (select i3.fbpid, i3.fuid, count(*) fsubgame_played_cnt from (select distinct i1.fbpid, i1.fuid, i1.fgame_id  from dim.reg_user_sub i1
        left join dim.bpid_map i2 on i1.fbpid = i2.fbpid where i1.dt = '%(statdate)s' and i2.fgamename = '地方棋牌') i3 group by i3.fbpid, i3.fuid) i
  on x.fbpid = i.fbpid and x.fuid = i.fuid
group by x.fuid;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_part1(sys.argv[1:])
a()
