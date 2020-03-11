#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_user_location_day(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_location_info
        (
            fdate            date,
            fgamefsk         bigint,
            fplatformfsk     bigint,
            fversionfsk      bigint,
            fterminalfsk     bigint,
            fip_country      varchar(128),
            fip_province     varchar(128),
            fip_city         varchar(128),
            fip_countrycode  varchar(32),
            fregusercnt      bigint,
            factusercnt      bigint,
            fpayusercnt      bigint,
            fincome          decimal(38,2)
        )
        partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        hql = """
          INSERT overwrite TABLE analysis.user_location_info partition(dt = '%(ld_begin)s')
          SELECT '%(ld_begin)s' fdate,
                    fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,
                    fip_country,
                    fip_province,
                    fip_city,
                    fip_countrycode,
                    count(DISTINCT CASE WHEN is_reg =1 THEN t.fuid ELSE NULL END) AS freguser,
                    count(DISTINCT CASE WHEN is_act =1 THEN t.fuid ELSE NULL END) AS factuser,
                    count(DISTINCT CASE WHEN fincome > 0 THEN t.fplatform_uid ELSE NULL END) AS fpayuser,
                    sum(fincome) AS fincome
             FROM
               (SELECT b.fgamefsk,
                       b.fplatformfsk,
                       b.fversionfsk ,
                       b.fterminalfsk,
                       uls.fip_country,
                       uls.fip_province,
                       uls.fip_city,

                       uls.fip_countrycode ,
                       a.fuid,
                       a.fbpid,
                       a.fplatform_uid ,
                       is_act ,
                       fincome,
                       is_reg
                FROM (
                      -- 活跃用户
                      SELECT fuid,
                             fbpid,
                             0 AS fplatform_uid ,
                             1 AS is_act ,
                             0 AS fincome,
                             0 AS is_reg
                      FROM stage.active_user_mid
                      WHERE dt = '%(ld_begin)s'
                      UNION ALL

                      --付费用户
                      SELECT max(b.fuid) AS fuid,
                             a.fbpid,
                             a.fplatform_uid,
                             0 AS is_act ,
                             round(sum(a.fcoins_num * a.frate), 6) fincome ,
                             0 AS is_reg
                      FROM stage.payment_stream_stg a
                      LEFT JOIN
                        (SELECT fbpid,
                                fplatform_uid,
                                max(fuid) AS fuid
                         FROM stage.pay_user_mid
                         GROUP BY fbpid,
                                  fplatform_uid) b ON a.fbpid=b.fbpid
                      AND a.fplatform_uid = b.fplatform_uid
                      WHERE a.dt = '%(ld_begin)s'
                      GROUP BY a.fplatform_uid,
                               a.fbpid
                      UNION ALL

                      --注册用户
                      SELECT fuid,
                             fbpid ,
                             0 AS fplatform_uid ,
                             0 AS is_act,
                             0 AS fincome,
                             1 AS is_reg
                      FROM stage.user_dim
                      WHERE dt = '%(ld_begin)s') a
                JOIN analysis.bpid_platform_game_ver_map b ON a.fbpid = b.fbpid
                LEFT JOIN
                  (SELECT fuid,
                          b.fgamefsk,
                          b.fversionfsk,
                          b.fterminalfsk,
                          fip_country,
                          fip_province,
                          fip_city,
                          fip,
                          fip_countrycode,
                          row_number() over(partition BY fuid,fgamefsk,fversionfsk,fterminalfsk
                                            ORDER BY flogin_at DESC) AS rn
                   FROM stage.user_login_stg c
                   JOIN analysis.bpid_platform_game_ver_map b ON c.fbpid = b.fbpid
                   AND c.dt = '%(ld_begin)s') uls ON a.fuid = uls.fuid
                AND b.fgamefsk = uls.fgamefsk
                AND b.fversionfsk = uls.fversionfsk
                AND b.fterminalfsk = uls.fterminalfsk
                AND uls.fip IS NOT NULL
                AND uls.rn = 1) t
             GROUP BY fgamefsk,
                      fplatformfsk,
                      fversionfsk,
                      fterminalfsk,
                      fip_country,
                      fip_province,
                      fip_city,
                      fip_countrycode
          ;
       """ % self.hql_dict
        hql_list.append(hql)

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
    a = agg_user_location_day(stat_date)
    a()
