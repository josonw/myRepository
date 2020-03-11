# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/service' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import sql_const


class agg_gameparty_total(BaseStat):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.gameparty_total
        (
            fdate                      date,
            fgamefsk                   bigint,
            fplatformfsk               bigint,
            fhallfsk                   bigint,
            fsubgamefsk                bigint,
            fterminaltypefsk           bigint,
            fversionfsk                bigint,
            fchannelcode               bigint,
            fusernum                   bigint, --玩牌人次
            fpartynum                  bigint, --牌局数
            fcharge                    decimal(20,4), --每局费用
            fplayusernum               bigint, --玩牌人数
            fregplayusernum            bigint, --当天注册用户玩牌人数
            fregpartynum               bigint, --当天注册用户玩牌人次
            fpaypartynum               bigint, --付费用户玩牌人次
            fpayusernum                bigint  --付费用户玩牌人数
        )
        partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res



    def stat(self):
        """ 重要部分，统计内容 """
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}
        numdate = self.stat_date.replace("-", "")

        query = sql_const.query_list(self.stat_date, alias_dic, None)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """
        drop table if exists work.gameparty_total_temp1_%(num_begin)s;
        create table work.gameparty_total_temp1_%(num_begin)s as
        select     fbpid,
                   fuid,
                   fgame_id,
                   fchannel_code,
                   sum(fparty_num) fusernum,
                   sum(fcharge) fcharge
            from dim.user_gameparty a
           where a.dt="%(ld_daybegin)s"
           group by fbpid,fuid,fgame_id,fchannel_code
              """%query[0]
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.gameparty_total_temp2_%(num_begin)s;
        create table work.gameparty_total_temp2_%(num_begin)s as
        select     fbpid,
                   finning_id,
                   ftbl_id,
                   fgame_id,
                   fchannel_code
            from dim.gameparty_stream a
           where a.dt="%(ld_daybegin)s"
           group by fbpid, finning_id, ftbl_id, fgame_id, fchannel_code
              """%query[0]
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        hql_list=[]
        for i in range(2):
            hql = """    -- 玩牌用户
              select %(select_field_str)s,
                     sum(fusernum) fusernum,
                     0 fpartynum,
                     sum(a.fcharge)  fcharge,
                     count(distinct a.fuid) fplayusernum,
                     count(distinct ud.fuid) fregplayusernum,
                     sum(case when ud.fuid is not null then fusernum end) fregpartynum,
                     0 fpaypartynum,
                     0 fpayusernum
                 from work.gameparty_total_temp1_%(num_begin)s a
            left join dim.reg_user_main_additional ud
                   on a.fbpid = ud.fbpid
                  and a.fuid = ud.fuid
                  and ud.dt = "%(ld_daybegin)s"
                 join dim.bpid_map b
               on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
            %(group_by)s
            """ % query[i]
            hql_list.append(hql)


        hql = """
        drop table if exists work.gameparty_total_temp3_%s;
        create table work.gameparty_total_temp3_%s as
        %s;
        insert into table work.gameparty_total_temp3_%s
        %s
              """%(numdate, numdate, hql_list[0], numdate, hql_list[1])
        res = self.hq.exe_sql(hql)
        if res != 0:return res


        hql_list=[]
        for i in range(2):
            hql = """    -- 牌局数
            insert into table work.gameparty_total_temp3_%(num_begin)s
              select %(select_field_str)s,
                     0 fusernum,
                     count(distinct concat_ws('0', finning_id, ftbl_id) )  fpartynum,
                     0 fcharge,
                     0 fplayusernum,
                     0 fregplayusernum,
                     0 fregpartynum,
                     0 fpaypartynum,
                     0 fpayusernum
                 from work.gameparty_total_temp2_%(num_begin)s a
                 join dim.bpid_map b
                   on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
               %(group_by)s
            """ % query[i]
            hql_list.append(hql)


        for i in range(2):
            hql = """    -- 付费用户玩牌人次，付费玩牌用户数
            insert into table work.gameparty_total_temp3_%(num_begin)s
              select %(select_field_str)s,
                     0 fusernum,
                     0 fpartynum,
                     0 fcharge,
                     0 fplayusernum,
                     0 fregplayusernum,
                     0 fregpartynum,
                     sum(fusernum) fpaypartynum,
                     count(distinct ud.fuid) fpayusernum
                from work.gameparty_total_temp1_%(num_begin)s a
                join dim.user_pay_day ud
                  on a.fbpid = ud.fbpid
                 and a.fuid = ud.fuid
                 and ud.dt = "%(ld_daybegin)s"
                join dim.bpid_map b
               on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
            %(group_by)s
            """ % query[i]
            hql_list.append(hql)


        hql = """ -- 把临时表数据处理后弄到正式表上
        insert overwrite table dcnew.gameparty_total
        partition( dt="%(ld_daybegin)s" )
            select "%(ld_daybegin)s" fdate,
                fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                sum(fusernum) fusernum,
                sum(fpartynum) fpartynum,
                sum(fcharge) fcharge,
                sum(fplayusernum) fplayusernum,
                sum(fregplayusernum) fregplayusernum,
                sum(fregpartynum) fregpartynum,
                sum(fpaypartynum) fpaypartynum,
                sum(fpayusernum) fpayusernum
            from work.gameparty_total_temp3_%(num_begin)s
            group by fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """ % query[0]
        hql_list.append(hql)
        res = self.exe_hql_list(hql_list)
        if res != 0:return res


            # 统计完清理掉临时表
        hql = """
        drop table if exists work.gameparty_total_temp1_%(num_begin)s;
        drop table if exists work.gameparty_total_temp2_%(num_begin)s;
        drop table if exists work.gameparty_total_temp3_%(num_begin)s;
        """ % query[0]
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        return res


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
a = agg_gameparty_total(statDate, eid)
a()
