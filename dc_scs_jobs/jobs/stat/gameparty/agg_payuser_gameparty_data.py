# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payuser_gameparty_data(BaseStat):
    def create_tab(self):

        hql = """
        use analysis;
        create table if not exists analysis.payuser_gameparty_game_type_fct
        (
          fdate           date,
          fgamefsk        bigint, --游戏编号
          fplatformfsk    bigint, --平台编号
          fversionfsk     bigint, --版本编号
          fterminalfsk    bigint, --终端编号
          fpname          varchar(100), --牌局类别，一级分类
          fplatform_uid   varchar(50),
          fuid            bigint,
          fpayplaynum     bigint, --付费用户玩牌人次
          fpayusercharge  decimal(20,4), --付费用户每局台费
          fpayuserprofit     bigint, --付费用户玩牌盈利
          fpayuserloss       bigint --付费用户玩牌亏损
        )
        partitioned by(dt string) ;
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


        hql = """    ----付费用户相关指标计算
        drop table if exists stage.payuser_gameparty_game_type_2_%(num_begin)s;
        create table stage.payuser_gameparty_game_type_2_%(num_begin)s as
          select    fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,
                    fpname,
                    fplatform_uid,
                    fuid,
                    sum(ut.fplaycnt) fpayplaynum,    --付费用户玩牌人次
                    round(sum(ut.fcharge), 4) fpayusercharge,   --付费用户每局台费
                    sum(ut.fpayuserprofit) fpayuserprofit,  --付费用户玩牌盈利
                    sum(ut.fpayuserloss) fpayuserloss   --付费用户玩牌亏损

                from (
                select a.fbpid, a.fpname, c.fplatform_uid, a.fuid, a.finning_id, a.ftbl_id, sum(a.fcharge) fcharge, count(1) fplaycnt,
                       sum(case when fgamecoins>=0 then fgamecoins else 0 end) fpayuserprofit,
                       sum(case when fgamecoins <0 then fgamecoins else 0 end) fpayuserloss
                       from stage.user_gameparty_stg a

                       join (SELECT fbpid, fplatform_uid, fuid
                                   FROM
                                      (SELECT fbpid, fplatform_uid, fuid,
                                                 row_number() over(partition BY fbpid,fplatform_uid) AS flag
                                          FROM stage.user_pay_info where dt='%(ld_begin)s' ) AS foo
                                       WHERE flag =1) c
                        on a.fbpid=c.fbpid and a.fuid=c.fuid

                    where a.dt='%(ld_begin)s'
                    group by a.fbpid, a.fpname, c.fplatform_uid, a.fuid, a.finning_id, a.ftbl_id
             ) ut
             join analysis.bpid_platform_game_ver_map bpm
               on ut.fbpid = bpm.fbpid
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fpname, fplatform_uid, fuid;
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.payuser_gameparty_game_type_fct partition( dt="%(ld_begin)s" )
            select "%(ld_begin)s" fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            coalesce(fpname,'未知') fpname,
            fplatform_uid,
            fuid,
            sum(fpayplaynum),
            sum(fpayusercharge),
            sum(fpayuserprofit),
            sum(fpayuserloss)

        from stage.payuser_gameparty_game_type_2_%(num_begin)s
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, coalesce(fpname,'未知'), fplatform_uid,fuid
        """ % self.hql_dict
        hql_list.append(hql)

        # 统计完清理掉临时表
        hql = """
          drop table if exists stage.payuser_gameparty_game_type_2_%(num_begin)s;
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
a = agg_payuser_gameparty_data(statDate, eid)
a()
