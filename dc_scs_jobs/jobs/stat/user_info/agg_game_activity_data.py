#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_game_activity_data(BaseStat):
    """活动数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.game_activity_dim
                (
                fsk bigint,
                fgamefsk bigint,
                fact_id varchar(50),
                fact_name varchar(100),
                fdis_name varchar(100),
                fdesc varchar(50),
                fsdate varchar(50),
                fedate varchar(50)
                )
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """create external table if not exists analysis.game_activity_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fact_id varchar(50),
                fusercnt bigint,
                fusernum bigint,
                fregusercnt bigint,
                fregusernum bigint,
                fpayusercnt bigint,
                fpayusernum bigint,
                ffpayusercnt bigint,
                ffpayusernum bigint,
                fincome decimal(20,2)
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
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res
        # 创建临时表
        hql = """
        insert overwrite table analysis.game_activity_fct
        partition (dt = '%(ld_daybegin)s')

            SELECT '%(ld_daybegin)s' fdate,
                                     fgamefsk,
                                     fplatformfsk,
                                     fversionfsk,
                                     fterminalfsk,
                                     fact_id,
                                     sum(fusercnt) fusercnt,
                                     sum(fusernum) fusernum,
                                     sum(fregusercnt) fregusercnt,
                                     sum(fregusernum) fregusernum,
                                     sum(fpayusercnt) fpayusercnt,
                                     sum(fpayusernum) fpayusernum,
                                     sum(ffpayusercnt) ffpayusercnt,
                                     sum(ffpayusernum) ffpayusernum,
                                     sum(fincome) fincome,
                                     fact_name
            FROM
              (SELECT fgamefsk,
                      fplatformfsk,
                      fversionfsk,
                      fterminalfsk,
                      fact_id,
                      max(a.fact_name) fact_name,
                      count(DISTINCT concat(a.fuid, flts_at)) fusercnt,
                      count(DISTINCT a.fuid) fusernum,
                      count(DISTINCT if(b.fuid IS NULL, NULL, concat(a.fuid, flts_at))) fregusercnt,
                      count(DISTINCT b.fuid) fregusernum,
                      count(DISTINCT if(c.fuid IS NULL, NULL, concat(a.fuid, flts_at))) fpayusercnt,
                      count(DISTINCT c.fuid) fpayusernum,
                      count(DISTINCT if(p.fuid IS NULL, NULL, concat(p.fuid, flts_at))) ffpayusercnt,
                      count(DISTINCT p.fuid) ffpayusernum,
                      0 fincome
               FROM stage.game_activity_stg a
               LEFT OUTER JOIN stage.user_dim b
               ON a.fbpid = b.fbpid
               AND a.fuid = b.fuid
               AND b.dt = '%(ld_daybegin)s'
               LEFT OUTER JOIN stage.user_pay_info c
               ON a.fuid = c.fuid
               AND a.fbpid = c.fbpid
               AND c.dt = '%(ld_daybegin)s'
               LEFT OUTER JOIN stage.pay_user_mid p
               ON a.fbpid = p.fbpid
               AND a.fuid = p.fuid
               AND p.dt = '%(ld_daybegin)s'
               JOIN analysis.bpid_platform_game_ver_map d
               ON a.fbpid = d.fbpid
               WHERE a.dt = '%(ld_daybegin)s'
               GROUP BY fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fterminalfsk,
                        fact_id
               UNION ALL
               SELECT fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,
                    fact_id,
                    fact_name,
                    0 fusercnt,
                    0 fusernum,
                    0 fregusercnt,
                    0 fregusernum,
                    0 fpayusercnt,
                    0 fpayusernum,
                    0 ffpayusercnt,
                    0 ffpayusernum,
                    round(sum(fcoins_num * frate), 2) fincome
               FROM
                 (SELECT fbpid,
                         fact_id,
                         fuid,
                         max(fact_name) fact_name
                  FROM stage.game_activity_stg
                  WHERE dt = '%(ld_daybegin)s'
                  GROUP BY fbpid,
                           fact_id,
                           fuid ) a
               JOIN
                 ( SELECT fbpid,
                          fplatform_uid,
                          max(fuid) AS fuid
                  FROM stage.pay_user_mid
                  GROUP BY fbpid,
                           fplatform_uid ) b
                ON a.fbpid = b.fbpid
               AND a.fuid = b.fuid
               JOIN stage.payment_stream_stg c
               ON a.fbpid = c.fbpid
               AND c.dt = '%(ld_daybegin)s'
               AND b.fplatform_uid = c.fplatform_uid
               JOIN analysis.bpid_platform_game_ver_map bpm
               ON bpm.fbpid = A.fbpid
               GROUP BY fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fterminalfsk,
                        fact_id,
                        fact_name) tmp
            GROUP BY fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     fterminalfsk,
                     fact_id,
                     fact_name;


        insert into table analysis.game_activity_dim
        select  0 fsk, a.fgamefsk, a.fact_id, a.fact_name, a.fact_name fdis_name,
                null fdesc,
                null fsdate,
                null fedate
        from (select b.fgamefsk, a.fact_id, max(a.fact_name) fact_name
                from stage.game_activity_stg a
               join analysis.bpid_platform_game_ver_map b
                 on a.fbpid=b.fbpid
               where a.dt = '%(ld_daybegin)s'
               group by b.fgamefsk, a.fact_id) a
        left outer join analysis.game_activity_dim b
         on a.fgamefsk=b.fgamefsk
        and a.fact_id=b.fact_id
        where b.fact_id is null
        group by a.fgamefsk, a.fact_id, a.fact_name;

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
a = agg_game_activity_data(statDate)
a()
