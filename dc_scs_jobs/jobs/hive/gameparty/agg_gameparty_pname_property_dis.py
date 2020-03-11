#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


class agg_gameparty_pname_property_dis(BaseStat):
    """ 牌局分析>>>底注场数据>>>资产分布登陆携带 """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_pname_property_dis
        (
          fdate              date,
          fgamefsk           bigint,
          fplatformfsk       bigint,
          fhallfsk           bigint,
          fsubgamefsk        bigint,
          fterminaltypefsk   bigint,
          fversionfsk        bigint,
          fchannelcode       bigint,
          fnum               bigint,
          fname              varchar(15),
          fpartyname         varchar(50),
          fusernum           bigint,
          fpname             varchar(100)
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_pname_property_dis'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}

        GROUPSET1 = {'alias':['src_tbl_alias', 'src_tbl_alias','src_tbl_alias', 'src_tbl_alias'],
                     'field':['fpname', 'fnum', 'fname', 'fpartyname'],
                     'comb_value':[[1, 1, 1, 1],
                                   [0, 1, 1, 1] ] }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        query = sql_const.query_list(self.stat_date, alias_dic, GROUPSET1)


        hql = """
              drop table if exists work.gameparty_pname_property_dis_%(num_begin)s;
              create table work.gameparty_pname_property_dis_%(num_begin)s
              as
                select a.fbpid,
                    case
                         when user_gamecoins <= 0 then 0 --存number
                         when user_gamecoins >= 1 and user_gamecoins < 5000 then 1
                         when user_gamecoins >= 5000 and user_gamecoins < 10000 then  5000
                         when user_gamecoins >= 10000 and user_gamecoins < 50000 then  10000
                         when user_gamecoins >= 50000 and user_gamecoins < 100000 then  50000
                         when user_gamecoins >= 100000 and user_gamecoins < 500000 then  100000
                         when user_gamecoins >= 500000 and user_gamecoins < 1000000 then  500000
                         when user_gamecoins >= 1000000 and user_gamecoins < 5000000 then  1000000
                         when user_gamecoins >= 5000000 and user_gamecoins < 10000000 then  5000000
                         when user_gamecoins >= 10000000 and user_gamecoins < 50000000 then  10000000
                         when user_gamecoins >= 50000000 and user_gamecoins < 100000000 then  50000000
                         when user_gamecoins >= 100000000 and user_gamecoins < 1000000000 then 100000000
                    else 1000000000 end fnum,
                    case
                         when user_gamecoins <= 0 then  '0' --存string
                         when user_gamecoins >= 1 and user_gamecoins < 5000 then '1-5000'
                         when user_gamecoins >= 5000 and user_gamecoins < 10000 then '5000-1万'
                         when user_gamecoins >= 10000 and user_gamecoins < 50000 then  '1万-5万'
                         when user_gamecoins >= 50000 and user_gamecoins < 100000 then '5万-10万'
                         when user_gamecoins >= 100000 and user_gamecoins < 500000 then '10万-50万'
                         when user_gamecoins >= 500000 and user_gamecoins < 1000000 then '50万-100万'
                         when user_gamecoins >= 1000000 and user_gamecoins < 5000000 then '100万-500万'
                         when user_gamecoins >= 5000000 and user_gamecoins < 10000000 then '500万-1000万'
                         when user_gamecoins >= 10000000 and user_gamecoins < 50000000 then '1000万-5000万'
                         when user_gamecoins >= 50000000 and user_gamecoins < 100000000 then '5000万-1亿'
                         when user_gamecoins >= 100000000 and user_gamecoins < 1000000000 then '1亿-10亿'
                    else '10亿+' end fname,
                    fante fpartyname, a.fuid, fpname,fgame_id,fchannel_code
                from (
                     select distinct fbpid, fuid, fblind_1 fante, fpname,
                     coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                     coalesce(cast (fchannel_code as int),%(null_int_report)d) fchannel_code
                      from stage.user_gameparty_stg
                      where dt="%(ld_daybegin)s"
                     ) a
                join (
                    select fbpid, fuid, user_gamecoins
                    from (
                        select fbpid, fuid, user_gamecoins,
                            row_number() over(partition by fbpid, fuid order by flogin_at, user_gamecoins asc) rown
                        from dim.user_login_additional
                        where dt="%(ld_daybegin)s"
                          ) ss
                    where ss.rown = 1
                     ) b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
              """%query[0]
        res = self.hq.exe_sql(hql)
        if res != 0:return res


        hql_list = []
        for i in range(2):
            hql = """ -- 每天各个盲注场产用户当天的首次登录资产分布
                select "%(ld_daybegin)s" fdate,
                    %(select_field_str)s,
                    fnum,
                    fname ,
                    fpartyname,
                    count(distinct fuid) fusernum,
                    coalesce(fpname,'%(null_str_group_rule)s') fpname
                from work.gameparty_pname_property_dis_%(num_begin)s a
                join dim.bpid_map b
                  on a.fbpid = b.fbpid
               where b.hallmode = %(hallmode)s
                %(group_by)s
            """    % query[i]
            hql_list.append(hql)


        hql = """
        insert overwrite table dcnew.gameparty_pname_property_dis
        partition( dt="%s" )
        %s;
        insert into table dcnew.gameparty_pname_property_dis
        partition( dt="%s" )
        %s
              """%(self.stat_date, hql_list[0], self.stat_date, hql_list[1])
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        res = self.hq.exe_sql("""drop table if exists work.gameparty_pname_property_dis_%(num_begin)s"""%query[0])
        if res != 0:return res

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
a = agg_gameparty_pname_property_dis(statDate, eid)
a()
