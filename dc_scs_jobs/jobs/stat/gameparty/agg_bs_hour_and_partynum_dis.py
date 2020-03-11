#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_bs_hour_and_partynum_dis(BaseStat):

    def create_tab(self):
        hql = '''
        use analysis;
        create table if not exists analysis.gameparty_bs_hour_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk    bigint,
          fversionfsk     bigint,
          fterminalfsk    bigint,
          fhour            varchar(20),
          fusernum        bigint

        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_bs_hour_fct'
        '''
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = '''
        use analysis;
        create table if not exists analysis.gameparty_bs_party_qj_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk    bigint,
          fversionfsk     bigint,
          fterminalfsk    bigint,
          fparty_qj       varchar(20),
          fusernum        bigint

        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_bs_party_qj_fct'
        '''
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res
    def stat(self):
        ''' 重要部分，统计内容 '''
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        fpartynumfsk = """
        case
            when fpartynum=0 then '0局'
            when fpartynum=1 then '1局'
            when 2<=fpartynum and fpartynum<=5 then '2-5局'
            when 6<=fpartynum and fpartynum<=10 then '6-10局'
            when 11<=fpartynum and fpartynum<=20 then '11-20局'
            when 21<=fpartynum and fpartynum<=30 then '21-30局'
            when 31<=fpartynum and fpartynum<=40 then '31-40局'
            when 41<=fpartynum and fpartynum<=50 then '41-50局'
            when 51<=fpartynum and fpartynum<=60 then '51-60局'
            when 61<=fpartynum and fpartynum<=70 then '61-70局'
            when 71<=fpartynum and fpartynum<=80 then '71-80局'
            when 81<=fpartynum and fpartynum<=90 then '81-90局'
            when 91<=fpartynum and fpartynum<=100 then '91-100局'
            when 101<=fpartynum and fpartynum<=150 then '101-150局'
            when 151<=fpartynum and fpartynum<=200 then '151-200局'
            when 201<=fpartynum and fpartynum<=300 then '201-300局'
            when 301<=fpartynum and fpartynum<=400 then '301-400局'
            when 401<=fpartynum and fpartynum<=500 then '401-500局'
            when 501<=fpartynum and fpartynum<=1000 then '501-1000局'
            else '1000+局'
        end"""

        query = { 'statdate':statDate,'fpartynumfsk':fpartynumfsk}

        res = self.hq.exe_sql('''use stage; set hive.exec.dynamic.partition.mode=nonstrict; ''')
        if res != 0: return res

        hql = """   INSERT overwrite TABLE analysis.gameparty_bs_hour_fct
                    partition(dt='%(statdate)s')
                    SELECT '%(statdate)s' fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                          concat(hour(fs_timer),'时') hour,
                          count(DISTINCT fuid) fusernum
                    FROM stage.user_gameparty_stg a
                    JOIN analysis.bpid_platform_game_ver_map b
                    ON a.fbpid = b.fbpid
                    WHERE a.dt = '%(statdate)s'
                      AND fpalyer_cnt != 0
                      AND fmatch_id IS NOT NULL
                      AND fmatch_id != '0'
                    GROUP BY fgamefsk,
                             fplatformfsk,
                             fversionfsk,
                             fterminalfsk,
                             concat(hour(fs_timer),'时')
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """ INSERT overwrite TABLE analysis.gameparty_bs_party_qj_fct
                    partition(dt='%(statdate)s')
                    SELECT fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                           %(fpartynumfsk)s fparty_qj,
                           count(DISTINCT fuid) fusernum
                    from
                      ( SELECT '%(statdate)s' fdate,
                                fgamefsk,
                                fplatformfsk,
                                fversionfsk,
                                fterminalfsk,
                                fuid,
                                count(1) fpartynum
                       FROM stage.user_gameparty_stg a
                       JOIN analysis.bpid_platform_game_ver_map b
                       ON a.fbpid = b.fbpid
                       WHERE a.dt = '%(statdate)s'
                         AND fpalyer_cnt != 0
                         AND fmatch_id IS NOT NULL
                         AND fmatch_id != '0'
                       GROUP BY fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fuid
                       ) foo
                    GROUP BY fdate,
                             fgamefsk,
                             fplatformfsk,
                             fversionfsk,
                             fterminalfsk,
                             %(fpartynumfsk)s
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res

#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_bs_hour_and_partynum_dis(statDate, eid)
a()
