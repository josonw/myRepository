# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

#牌局日志的玩牌人数被强制置为  0,牌局人数 指标在此表中作废

class agg_gameparty_mid_data(BaseStat):
    def create_tab(self):

        hql = """
        use analysis;
        CREATE TABLE IF NOT EXISTS analysis.user_gameparty_mid (
                fdate           date
                ,fgamefsk        bigint --  游戏编号
                ,fplatformfsk   bigint --  平台编号
                ,fversionfsk    bigint --  版本编号
                ,fterminalfsk   bigint --  终端编号
                ,fcharge        decimal(30,2) --台费
                ,flose          bigint --  输钱
                ,fwin          bigint --  赢钱
                ,falltime       bigint --  游戏时长（分）
                ,fpartytime     bigint --  牌局时长（分）
                ,fpartynum      int -- 局数
                ,fplayusernum   bigint --  人数
                ,fusernum       bigint --  人次
                ,fplayer_cnt     int -- 牌局人数
                ,fpname          varchar(200) --牌局类别 一级分类
                ,fsubname        varchar(200) --牌局二级分类
                ,fgsubname       varchar(200) --牌局三级分类
                ,ftbuymin        int -- 最小携带
                ,ftbuymax        int -- 最大买入
        ) partitioned by(dt date);
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.hq.debug = 0
        hql_list = []

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
        INSERT overwrite TABLE analysis.user_gameparty_mid partition(dt="%(ld_begin)s")
            SELECT "%(ld_begin)s" AS fdate,
                   fgamefsk ,
                   fplatformfsk ,
                   fversionfsk ,
                   fterminalfsk ,
                   sum(ugs.fcharge) AS fcharge ,
                   sum(flose) AS flose,
                   sum(fwin) AS fwin ,
                   sum(sts) AS falltime ,
                   sum(ts) AS fpartytime ,
                   sum(fpartynum) AS fpartynum,
                   sum(fplayusernum) AS fplayusernum ,
                   sum(fusernum) AS fusernum ,
                   0 as fpalyer_cnt ,
                   fpname ,
                   fsubname ,
                   fgsubname ,
                   ftbuymin ,
                   ftbuymax
            FROM
              ( SELECT fbpid AS fbpid,
                       0 AS fpalyer_cnt,
                       fpname AS fpname,
                       fsubname AS fsubname,
                       fgsubname AS fgsubname ,
                       ftbuymin AS ftbuymin ,
                       ftbuymax AS ftbuymax,
                       sum(fpartynum) AS fpartynum,
                       sum(fusernum) AS fusernum ,
                       sum(fplayusernum) AS fplayusernum,
                       sum(flose) AS flose ,
                       sum(fwin) AS fwin ,
                       sum(fcharge) AS fcharge,
                       sum(sts) AS sts,
                       sum(ts) AS ts
               FROM
                 ( SELECT fbpid,
                          0 as fpalyer_cnt,
                          fpname,
                          fsubname,
                          fgsubname ,
                          ftbuymin ,
                          ftbuymax,
                          count(DISTINCT concat_ws('0', ftbl_id, finning_id)) AS fpartynum,
                          count(fuid) AS fusernum ,
                          count(DISTINCT fuid) AS fplayusernum,
                          sum(CASE WHEN fgamecoins < 0 THEN abs(fgamecoins) ELSE 0 END) AS flose ,
                          sum(CASE WHEN fgamecoins > 0 THEN fgamecoins ELSE 0 END) AS fwin ,
                          sum(fcharge) AS fcharge,
                          sum(CASE WHEN fs_timer = '1970-01-01 00:00:00' THEN 0 WHEN fe_timer = '1970-01-01 00:00:00' THEN 0 ELSE unix_timestamp(fe_timer)-unix_timestamp(fs_timer) END) sts,
                          0 AS ts
                  FROM stage.user_gameparty_stg
                  WHERE dt = "%(ld_begin)s"
                    AND fpalyer_cnt!=0
                  GROUP BY fbpid,
                           fpname ,
                           fsubname ,
                           fgsubname ,
                           ftbuymin ,
                           ftbuymax
                  UNION ALL
                  SELECT fbpid,
                                   0 as fpalyer_cnt,
                                   fpname,
                                   fsubname,
                                   fgsubname ,
                                   ftbuymin ,
                                   ftbuymax,
                                   0 AS fpartynum,
                                   0 AS fusernum ,
                                   0 AS fplayusernum,
                                   0 AS flose ,
                                   0 AS fwin ,
                                   0 AS fcharge,
                                   0 sts,
                                   max(CASE WHEN fs_timer = '1970-01-01 00:00:00' THEN 0 WHEN fe_timer = '1970-01-01 00:00:00' THEN 0 ELSE unix_timestamp(fe_timer)-unix_timestamp(fs_timer) END) AS ts
                  FROM stage.user_gameparty_stg
                  WHERE dt = "%(ld_begin)s"
                    AND fpalyer_cnt!=0
                  GROUP BY fbpid,
                           fpname ,
                           fsubname ,
                           fgsubname ,
                           ftbuymin ,
                           ftbuymax ,
                           ftbl_id,
                           finning_id
                           ) t1
               GROUP BY fbpid,
                        fpname ,
                        fsubname ,
                        fgsubname ,
                        ftbuymin ,
                        ftbuymax ) ugs
            JOIN analysis.bpid_platform_game_ver_map bpm ON ugs.fbpid = bpm.fbpid
            GROUP BY fgamefsk ,
                     fplatformfsk ,
                     fversionfsk ,
                     fterminalfsk ,
                     fpname ,
                     fsubname ,
                     fgsubname ,
                     ftbuymin ,
                     ftbuymax ;
        """ % self.hql_dict
        hql_list.append(hql)

        result = self.exe_hql_list(hql_list)
        return result


# 愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else:
    args = sys.argv[1].split(',')
    statDate = args[0]


# 生成统计实例
a = agg_gameparty_mid_data(statDate, eid)
a()
