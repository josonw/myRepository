#!/user/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_top_pay_data(BaseStat):
    """ 供top100付费使用的数据，记录每天每个用户的充值消费记录
    """
    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.top_pay_fct
        (
           fdate    date    ,
            fgamefsk    bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk bigint  ,
            fterminalfsk    bigint   ,
            fplatform_uid  string   ,
            fuid bigint,
            fpaycnt  int,
            dip   decimal(20,2),
            flognum int,
            fplaynum int,
            fgc_in  bigint,
            fgc_out bigint

        )
        partitioned by(dt date)
        location '/dw/analysis/top_pay_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


    def stat(self):

        query = { 'statdate':statDate,'ld_1daylater':PublicFunc.add_days(statDate, 1)}

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res


        hql = """insert overwrite table analysis.top_pay_fct
                    partition(dt="%(statdate)s")
                       select "%(statdate)s" fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                           m1.fplatform_uid,
                           m1.fuid,
                           m1.fpaycnt,
                           round(m1.dip,2) dip,
                           m2.flognum,
                           m3.fplaynum,
                           m4.fgc_in,
                           m4.fgc_out
                      from (select a.fbpid,
                                   a.fplatform_uid,
                                   case when max(a.fuid)>0 then max(a.fuid) else  max(e.fuid) end fuid,
                                   sum(fcoins_num * frate) dip,
                                   count(1) fpaycnt
                              from stage.payment_stream_stg a
                              left join (select fbpid, fplatform_uid,max(fuid) fuid
                                           from stage.pay_user_mid
                                           group by fbpid, fplatform_uid) e
                                on e.fbpid = a.fbpid
                               and e.fplatform_uid = a.fplatform_uid
                             where a.dt = "%(statdate)s"
                             group by a.fbpid, a.fplatform_uid) m1 --付费
                      left join (select fbpid, fuid, count(fuid) flognum
                                   from stage.user_login_stg
                                  where dt = "%(statdate)s"
                                  group by fbpid, fuid) m2 --登录次数
                        on m1.fbpid = m2.fbpid
                       and m1.fuid = m2.fuid
                      left join (select fbpid, fuid,
                                 case when sum(fplaynum1)>0 then sum(fplaynum1) else sum(fplaynum2) end fplaynum
                                from (
                                     select fbpid, fuid, count(1) fplaynum1,0 fplaynum2
                                       from stage.user_gameparty_stg
                                      where dt = "%(statdate)s"
                                      group by fbpid, fuid
                                      union all
                                      select fbpid, fuid, 0 fplaynum1, count(1) fplaynum2
                                       from stage.finished_gameparty_uid_mid
                                      where dt = "%(statdate)s"
                                      group by fbpid, fuid
                                      ) a
                                  group by fbpid, fuid) m3 --玩牌局数
                        on m1.fbpid = m3.fbpid
                       and m1.fuid = m3.fuid
                      left join (select fbpid,
                                        fuid,
                                        sum(case act_type
                                              when 1 then
                                               act_num
                                            end) fgc_in,
                                        sum(case act_type
                                              when 2 then
                                               abs(act_num)
                                            end) fgc_out
                                   from stage.pb_gamecoins_stream_stg
                                  where dt = "%(statdate)s"
                                  group by fbpid, fuid) m4
                        on m1.fbpid = m4.fbpid
                       and m1.fuid = m4.fuid
                      join analysis.bpid_platform_game_ver_map bpm
                        on m1.fbpid = bpm.fbpid

                    """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res



#愉快的统计要开始啦
global statDate
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
#生成统计实例
a = agg_top_pay_data(statDate)
a()
