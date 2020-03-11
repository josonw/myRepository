#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: SkyShen
# @Date:   2015-11-18 16:25:34
# @Last Modified by:   SkyShen
# @Last Modified time: 2015-11-20 15:26:54
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

class agg_gameparty_prechip(BaseStat):
    def create_tab(self):

        hql = """
        use analysis;
        CREATE TABLE IF NOT EXISTS analysis.user_gameparty_prechip_fct (
                fdate           date
                ,fgamefsk       bigint        --  游戏编号
                ,fplatformfsk   bigint        --  平台编号
                ,fversionfsk    bigint        --  版本编号
                ,fterminalfsk   bigint        --  终端编号
                ,fpname         varchar(200)  --  牌局类别 一级分类
                ,fsubname       varchar(200)  --  牌局二级分类
                ,fgsubname      varchar(200)  --  牌局三级分类
                ,fprechip       bigint       --  必下注数额
                ,fblind_1       bigint       --  底注（非德州）/ 小盲注额度（德州）
                ,fcharge        decimal(30,2) --  台费
                ,flose          bigint        --  输钱
                ,fwin           bigint        --  赢钱
                ,falltime       bigint        --  游戏时长（分）
                ,fpartytime     bigint        --  牌局时长（分）
                ,fpartynum      int           --  局数
                ,fplayusernum   bigint        --  人数
                ,fusernum       bigint        --  人次
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
        INSERT overwrite TABLE analysis.user_gameparty_prechip_fct
        partition(dt="%(ld_begin)s")
            SELECT "%(ld_begin)s" AS fdate,
                   fgamefsk ,
                   fplatformfsk ,
                   fversionfsk ,
                   fterminalfsk ,
                   fpname ,
                   fsubname ,
                   fgsubname ,
                   fprechip ,
                   fblind_1 ,
                   sum(fcharge) as fcharge  ,
                   sum(flose) as flose ,
                   sum(fwin) as fwin  ,
                   sum(falltime) as falltime  ,
                   sum(fpartytime) as fpartytime  ,
                   sum(fpartynum) as fpartynum ,
                   sum(fplayusernum) as fplayusernum  ,
                   sum(fusernum) as fusernum
            FROM
              ( SELECT fbpid AS fbpid,
                       fpname AS fpname,
                       fsubname AS fsubname,
                       fgsubname AS fgsubname ,
                       fprechip as fprechip,
                       fblind_1 as fblind_1 ,
                       sum(fpartynum) AS fpartynum,
                       sum(fusernum) AS fusernum ,
                       sum(fplayusernum) AS fplayusernum,
                       sum(flose) AS flose ,
                       sum(fwin) AS fwin ,
                       sum(fcharge) AS fcharge,
                       sum(falltime) AS falltime,
                       sum(fpartytime) AS fpartytime
               FROM
                 ( SELECT fbpid,
                          fpname,
                          fsubname,
                          fgsubname ,
                          fprechip ,
                          fblind_1,
                          count(DISTINCT concat_ws('0', ftbl_id, finning_id)) AS fpartynum,
                          count(fuid) AS fusernum ,
                          count(DISTINCT fuid) AS fplayusernum,
                          sum(CASE WHEN fgamecoins < 0 THEN abs(fgamecoins) ELSE 0 END) AS flose ,
                          sum(CASE WHEN fgamecoins > 0 THEN fgamecoins ELSE 0 END) AS fwin ,
                          sum(fcharge) AS fcharge,
                          sum(CASE WHEN fs_timer = '1970-01-01 00:00:00' THEN 0 WHEN fe_timer = '1970-01-01 00:00:00' THEN 0 ELSE unix_timestamp(fe_timer)-unix_timestamp(fs_timer) END) falltime,
                          0 AS fpartytime
                  FROM stage.user_gameparty_stg
                  WHERE dt = "%(ld_begin)s"
                    AND fpalyer_cnt!=0
                  GROUP BY fbpid,
                           fpname ,
                           fsubname ,
                           fgsubname ,
                           fprechip,
                           fblind_1
                  UNION ALL
                  SELECT fbpid,
                                   fpname,
                                   fsubname,
                                   fgsubname ,
                                   fprechip ,
                                   fblind_1,
                                   0 AS fpartynum,
                                   0 AS fusernum ,
                                   0 AS fplayusernum,
                                   0 AS flose ,
                                   0 AS fwin ,
                                   0 AS fcharge,
                                   0 falltime,
                                   max(CASE WHEN fs_timer = '1970-01-01 00:00:00' THEN 0 WHEN fe_timer = '1970-01-01 00:00:00' THEN 0 ELSE unix_timestamp(fe_timer)-unix_timestamp(fs_timer) END) AS fpartytime
                  FROM stage.user_gameparty_stg
                  WHERE dt = "%(ld_begin)s"
                    AND fpalyer_cnt!=0
                  GROUP BY fbpid,
                           fpname ,
                           fsubname ,
                           fgsubname ,
                           fprechip ,
                           fblind_1,
                           ftbl_id,
                           finning_id
                           ) t1
               GROUP BY fbpid,
                        fpname ,
                        fsubname ,
                        fgsubname ,
                        fprechip,
                        fblind_1 ) ugs
            JOIN analysis.bpid_platform_game_ver_map bpm
            --必下统计 只统计德州
            ON ugs.fbpid = bpm.fbpid and fgamefsk = 1396894
            GROUP BY fgamefsk ,
                     fplatformfsk ,
                     fversionfsk ,
                     fterminalfsk ,
                     fpname ,
                     fsubname ,
                     fgsubname ,
                     fprechip,
                     fblind_1
                      ;
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
a = agg_gameparty_prechip(statDate, eid)
a()
