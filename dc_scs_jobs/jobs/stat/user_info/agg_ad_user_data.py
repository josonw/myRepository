#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_ad_user_data(BaseStat):
    """广告用户数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.advertise_user_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fad_name varchar(50),
                fad_code varchar(50),
                fregusercnt bigint,
                factusercnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update(date)

        hql = """use stage;
                 set hive.auto.convert.join=false;
                 set io.sort.mb=256;
              """

        # 创建临时表
        hql += """
        insert overwrite table analysis.advertise_user_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fad_code fad_name,
                 fad_code,
                 max(fregusercnt) fregusercnt,
                 max(factusercnt) factusercnt
            from (select /*+ mapjoin(c) */
                         c.fgamefsk,
                         c.fplatformfsk,
                         c.fversionfsk,
                         c.fterminalfsk,
                         a.fad_code,
                         count(distinct fuid) fregusercnt,
                         0 factusercnt
                    from stage.user_dim a
                    join analysis.bpid_platform_game_ver_map c
                      on a.fbpid = c.fbpid
                    where a.dt = '%(ld_daybegin)s'
                     and  a.fad_code is not null
                   group by c.fgamefsk,
                            c.fplatformfsk,
                            c.fversionfsk,
                            c.fterminalfsk,
                            a.fad_code
                  union all
                  select /*+ mapjoin(d) */
                         d.fgamefsk,
                         d.fplatformfsk,
                         d.fversionfsk,
                         d.fterminalfsk,
                         b.fad_code,
                         0 fregusercnt,
                         count(distinct a.fuid) factusercnt
                    from stage.active_user_mid a
                    join stage.user_dim b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and b.fad_code is not null
                    join analysis.bpid_platform_game_ver_map d
                      on a.fbpid = d.fbpid
                    where a.dt = '%(ld_daybegin)s'
                   group by d.fgamefsk,
                            d.fplatformfsk,
                            d.fversionfsk,
                            d.fterminalfsk,
                            b.fad_code) tmp
           group by fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,
                    fad_code;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

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
a = agg_ad_user_data(statDate)
a()
