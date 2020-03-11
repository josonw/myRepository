#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_match_online_dis(BaseStat):

    def create_tab(self):
        hql = '''
        use analysis;
        create table if not exists analysis.gameparty_match_online_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk    bigint,
          fversionfsk     bigint,
          fterminalfsk    bigint,
          fonline          varchar(20),
          fusernum        bigint

        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_match_online_fct'
        '''
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


    def stat(self):
        ''' 重要部分，统计内容 '''
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        query = { 'statdate':statDate}

        res = self.hq.exe_sql('''use stage; set hive.exec.dynamic.partition.mode=nonstrict; ''')
        if res != 0: return res

        hql = """
        insert overwrite table analysis.gameparty_match_online_fct
        partition( dt='%(statdate)s' )

       select "%(statdate)s" fdate,
                   fgamefsk,
              fplatformfsk,
              fversionfsk,
              fterminalfsk,
              case
                when sts >= 0 and sts < 5 then
                 '0-5分钟'
                when sts >= 5 and sts < 15 then
                 '5-15分钟'
                when sts >= 15 and sts < 30 then
                 '15-30分钟'
                when sts >= 30 and sts < 60 then
                 '30-60分钟'
                when sts >= 60 and sts < 80 then
                 '60-80分钟'
                when sts >= 80 and sts < 120 then
                 '80-120分钟'
                when sts >= 120 and sts < 240 then
                 '2-4小时'
                when sts >= 240 then
                 '4小时以上'
              end as fonline,
              count(distinct fuid) fusernum
         from (select b.fgamefsk fgamefsk,
                      b.fplatformfsk fplatformfsk,
                      b.fversionfsk fversionfsk,
                      b.fterminalfsk fterminalfsk,
                      fuid,
                      sum(case when fs_timer = '1970-01-01 00:00:00' then 0 when fe_timer = '1970-01-01 00:00:00' then 0
                          else (unix_timestamp(fe_timer) - unix_timestamp(fs_timer)) / 60.0 end) sts
                from stage.user_gameparty_stg a
                 join analysis.bpid_platform_game_ver_map b
                   on a.fbpid = b.fbpid
                where a.dt = '%(statdate)s'
                  and fpalyer_cnt != 0
                  and fmatch_id is not null
                  and fmatch_id != '0'
                group by b.fgamefsk,
                         b.fplatformfsk,
                         b.fversionfsk,
                         b.fterminalfsk,
                         fuid) as aa
        group by fgamefsk,
                  fplatformfsk,
                  fversionfsk,
                  fterminalfsk,
                  case
                    when sts >= 0 and sts < 5 then
                     '0-5分钟'
                    when sts >= 5 and sts < 15 then
                     '5-15分钟'
                    when sts >= 15 and sts < 30 then
                     '15-30分钟'
                    when sts >= 30 and sts < 60 then
                     '30-60分钟'
                    when sts >= 60 and sts < 80 then
                     '60-80分钟'
                    when sts >= 80 and sts < 120 then
                     '80-120分钟'
                    when sts >= 120 and sts < 240 then
                     '2-4小时'
                    when sts >= 240 then
                     '4小时以上'
                  end
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
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
a = agg_gameparty_match_online_dis(statDate, eid)
a()
