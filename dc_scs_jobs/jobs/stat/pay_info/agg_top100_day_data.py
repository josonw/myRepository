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


class agg_top100_day_data(BaseStat):
    """ 记录每天前100用户的充值消费
    """
    def create_tab(self):

        hql = """
        use analysis;
        create table if not exists analysis.top100_pay_fct
        (
            fdate    date    ,
            fgamefsk    bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk bigint  ,
            fterminalfsk    bigint   ,
            ftop_payun int,
            ftop_playun int,
            top_dip   decimal(20,2),
            ftopgc_in  bigint,
            ftopgc_out bigint,
            fpayun int,
            dip decimal(20,2)
            
        )
        partitioned by(dt date)
        location '/dw/analysis/top100_pay_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res   

    def stat(self):

        query = { 'statdate':statDate,'ld_1daylater':PublicFunc.add_days(statDate, 1)}

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.top100_pay_fct
        partition(dt="%(statdate)s")
            select fdate,
                   a.fgamefsk,
                   a.fplatformfsk,
                   a.fversionfsk,a.fterminalfsk,
                   ftop_payun,
                   ftop_playun,
                   top_dip,
                   ftopgc_in,
                   ftopgc_out,
                   fpayun,
                   dip
              from (select "%(statdate)s" fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,fterminalfsk,
                           count(fuid) ftop_payun,
                           count(fplaynum) ftop_playun,
                           sum(dip) top_dip,
                           sum(fgc_in) ftopgc_in,
                           sum(fgc_out) ftopgc_out
                      from (select fgamefsk,
                                   fplatformfsk,
                                   fversionfsk,fterminalfsk,
                                   fuid,
                                   dip,
                                   fplaynum,
                                   fgc_in,
                                   fgc_out,
                                   row_number() over(partition by fgamefsk, fplatformfsk, fversionfsk order by dip desc) as flag
                              from analysis.top_pay_fct
                             where fdate = "%(statdate)s"
                             and fuid is not null) as foo
                     where flag <= 100
                     group by fgamefsk,
                                fplatformfsk,
                                fversionfsk,fterminalfsk) a
              left join (select fgamefsk,
                                fplatformfsk,
                                fversionfsk,fterminalfsk,
                                sum(dip) dip,
                                count(1) fpayun
                           from analysis.top_pay_fct
                          where fdate = "%(statdate)s"
                          group by fgamefsk,
                                fplatformfsk,
                                fversionfsk,fterminalfsk) b
                on a.fgamefsk = b.fgamefsk
               and a.fplatformfsk = b.fplatformfsk
               and a.fversionfsk = b.fversionfsk



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
a = agg_top100_day_data(statDate)
a()
