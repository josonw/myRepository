#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_type_add(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_type_fct_add
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk    bigint,
          fversionfsk     bigint,
          fterminalfsk    bigint,
          fpname          varchar(200),
          fsubname        varchar(200),
          fjoin_fee        bigint,
          fjoin_num        bigint,
          fjoin__cnt       bigint,
          fserv_fee        bigint,
          fregplayusernum  bigint,
          fmatch_cnt       bigint,
          fpayusernum      bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_type_fct_add'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        query = { 'statdate':statDate, "num_begin": statDate.replace('-', '')}

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """

        drop table if exists stage.agg_gameparty_type_add_1_%(num_begin)s;
          create table stage.agg_gameparty_type_add_1_%(num_begin)s as
            SELECT b.fgamefsk fgamefsk,
                   b.fplatformfsk fplatformfsk,
                   b.fversionfsk fversionfsk,
                   b.fterminalfsk fterminalfsk,
                   a.fpname,
                   a.fsubname,
                   0 fjoin_fee,
                   0 fjoin_num,
                   0 fjoin__cnt,
                   sum(fserv_charge) fserv_fee,
                   count(DISTINCT c.fuid) fregplayusernum,
                   count(DISTINCT CASE WHEN fmatch_id='0' THEN NULL ELSE fmatch_id END) fmatch_cnt,
                   count(DISTINCT d.fplatform_uid) fpayusernum
            FROM stage.user_gameparty_stg a
            LEFT JOIN stage.user_dim c ON c.fbpid = a.fbpid
            AND c.fuid = a.fuid
            AND c.dt="%(statdate)s"
            LEFT JOIN stage.user_pay_info d ON d.fbpid = a.fbpid
            AND d.fuid = a.fuid
            AND d.dt="%(statdate)s"
            JOIN analysis.bpid_platform_game_ver_map b ON a.fbpid = b.fbpid
            WHERE a.dt="%(statdate)s"
              AND fpalyer_cnt != 0
            GROUP BY b.fgamefsk,
                     b.fplatformfsk,
                     b.fversionfsk,
                     b.fterminalfsk,
                     fpname,
                     fsubname

        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists stage.agg_gameparty_type_add_2_%(num_begin)s;
          create table stage.agg_gameparty_type_add_2_%(num_begin)s as
            SELECT b.fgamefsk fgamefsk,
                   b.fplatformfsk fplatformfsk,
                   b.fversionfsk fversionfsk,
                   b.fterminalfsk fterminalfsk,
                   a.fpname fpname,
                   a.fname fsubname,
                   sum(fentry_fee) fjoin_fee,
                   count(DISTINCT a.fuid) fjoin_num,
                   count(a.fuid) fjoin__cnt,
                   0 fserv_fee,
                   0 fregplayusernum,
                   count(DISTINCT CASE WHEN fmatch_id='0' THEN NULL ELSE fmatch_id END) fmatch_cnt,
                   0 fpayusernum
            FROM stage.join_gameparty_stg a
            JOIN analysis.bpid_platform_game_ver_map b ON a.fbpid = b.fbpid
            WHERE a.dt="%(statdate)s"
            GROUP BY b.fgamefsk,
                     b.fplatformfsk,
                     b.fversionfsk,
                     b.fterminalfsk,
                     a.fpname ,
                     a.fname

        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        INSERT overwrite TABLE analysis.gameparty_type_fct_add partition(dt="%(statdate)s")
        SELECT "%(statdate)s" fdate,
               a.fgamefsk,
               a.fplatformfsk,
               a.fversionfsk,
               a.fterminalfsk,
               a.fpname,
               a.fsubname,
               coalesce(sum(fjoin_fee),0) fjoin_fee ,
               coalesce(sum(fjoin_num),0) fjoin_num ,
               coalesce(sum(fjoin__cnt),0) fjoin__cnt ,
               coalesce(sum(fserv_fee),0) fserv_fee ,
               sum(fregplayusernum),
               sum(fmatch_cnt) ,
               sum(fpayusernum)
        FROM
          (SELECT * FROM stage.agg_gameparty_type_add_1_%(num_begin)s
           UNION ALL
           SELECT * FROM stage.agg_gameparty_type_add_2_%(num_begin)s ) a
        GROUP BY a.fgamefsk,
                 a.fplatformfsk,
                 a.fversionfsk,
                 a.fterminalfsk,
                 a.fpname,
                 a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

         # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gameparty_type_add_1_%(num_begin)s;
        drop table if exists stage.agg_gameparty_type_add_2_%(num_begin)s;

        """ % query
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
a = agg_gameparty_type_add(statDate, eid)
a()
