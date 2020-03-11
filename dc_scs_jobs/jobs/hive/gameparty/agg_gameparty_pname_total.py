# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_gameparty_pname_total(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.gameparty_pname_total
        (
          fdate               date,
          fgamefsk            bigint,
          fplatformfsk        bigint,
          fhallfsk            bigint,
          fsubgamefsk         bigint,
          fterminaltypefsk    bigint,
          fversionfsk         bigint,
          fchannelcode        bigint,

          fpname              varchar(100),    --牌局类别，一级分类
          fusernum            bigint,          --玩牌人次
          fpartynum           bigint,          --牌局数
          fcharge             decimal(20,4),   --每局费用

          fplayusernum        bigint,          --玩牌人数
          fregplayusernum     bigint,          --当天注册用户玩牌人次
          factplayusernum     bigint,          --当天活跃用户玩牌人次
          fpaypartynum        bigint,          --付费用户且玩牌场次
          fpayusernum         bigint,          --付费用户且玩牌用户数

          fbankruptusercnt    bigint,          --当天破产用户数
          fbankruptuserradio  decimal(10,4),   --破产率
          frelievecnt         bigint,          --当天领取救济用户数
          frelieveradio       decimal(10,4),   --救济率

          fuserprofit         bigint,          --玩牌盈利
          fuserloss           bigint,          --玩牌亏损
          fpayplaynum         bigint,          --付费用户玩牌人次
          fpayusercharge      decimal(20,4),   --付费用户每局费用
          fpayuserprofit      bigint,          --付费用户玩牌盈利
          fpayuserloss        bigint           --付费用户玩牌亏损
        )
        partitioned by(dt date) ;
        """

        res = self.hq.exe_sql(hql)
        if res != 0:return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.hq.debug = 0
        hql_list = []

        extend_group = {'fields':['fpname'],
                        'groups':[[1], [0]] }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """
        drop table if exists work.gameparty_pname_total_temp2_%(statdatenum)s;
        create table work.gameparty_pname_total_temp2_%(statdatenum)s as
            select
                a.fgamefsk,
                a.fplatformfsk,
                a.fhallfsk,
                a.fterminaltypefsk,
                a.fversionfsk,
                a.fgame_id,
                a.fchannel_code,
                a.fpname,
                a.fuid,
                a.fcharge,
                a.fparty_num,
                a.fwin_amt,
                a.flose_amt,
                a.hallmode,
                a.fbpid,
                case when c.fplatform_uid is null then 0 else 1 end ispayuser
           from dim.user_gameparty a
           left join (SELECT fbpid, max(fplatform_uid) fplatform_uid, fuid
                        FROM
                            (SELECT fbpid, fplatform_uid, fuid,
                                    row_number() over(partition BY fbpid,fplatform_uid) AS flag
                            FROM dim.user_pay_day where dt='%(statdate)s' ) AS foo
                      WHERE flag =1
                      group by fbpid, fuid
                      ) c
            on a.fbpid=c.fbpid and a.fuid=c.fuid
            where a.dt='%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """-- 玩牌用户
          select fgamefsk,
                 coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                 coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                 coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                 coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                 coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                 coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                 coalesce(ut.fpname, '%(null_str_group_rule)s') fpname,
                 sum(fparty_num) fusernum,
                 0 fpartynum,
                 round(sum(ut.fcharge), 4) fcharge,

                 count(distinct ut.fuid) fplayusernum,
                 0 fregplayusernum,
                 0 factplayusernum,
                 0 fpaypartynum,  --付费用户牌局数
                 count(distinct case when ispayuser=1 then ut.fuid else null end ) fpayusernum,   --付费用户且玩牌用户数

                 0 fbankruptusercnt    ,  --当天破产用户数
                 0 fbankruptuserradio  , --破产率
                 0 frelievecnt     , --当天领取救济用户数
                 0 frelieveradio,    --救济率

                 sum(ut.fwin_amt) fuserprofit,
                 sum(ut.flose_amt) fuserloss,
                 sum(ut.fparty_num*ispayuser) fpayplaynum,    --付费用户玩牌人次
                 round(sum(ut.fcharge*ispayuser), 4) fpayusercharge,   --付费用户每局费用
                 sum(ut.fwin_amt*ispayuser) fpayuserprofit,  --付费用户玩牌盈利
                 sum(ut.flose_amt*ispayuser) fpayuserloss   --付费用户玩牌亏损
             from work.gameparty_pname_total_temp2_%(statdatenum)s ut
            where ut.hallmode=%(hallmode)s
            group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fpname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        drop table if exists work.gameparty_pname_total_temp_%(statdatenum)s;
        create table work.gameparty_pname_total_temp_%(statdatenum)s as
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        -- 新老牌局整合 PS:直接去掉了老牌局数据
          select  fgamefsk,
                  coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                  coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                  coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                  coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                  coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                  coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                    coalesce(a.fpname, '%(null_str_group_rule)s') fpname,
                    0 fusernum,
                    count(distinct concat_ws('0', finning_id, ftbl_id) ) fpartynum,
                    0 fcharge,

                    0 fplayusernum,
                    0 fregplayusernum,
                    0 factplayusernum,
                    0 fpaypartynum,
                    0 fpayusernum,

                    0 fbankruptusercnt    ,  --当天破产用户数
                    0 fbankruptuserradio  , --破产率
                    0 frelievecnt     , --当天领取救济用户数
                    0 frelieveradio,    --救济率

                    0 fuserprofit,
                    0 fuserloss,
                    0 fpayplaynum,
                    0 fpayusercharge,
                    0 fpayuserprofit,
                    0 fpayuserloss
                from dim.gameparty_stream a
               where a.dt = "%(statdate)s" and a.hallmode=%(hallmode)s
               group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fpname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert into table work.gameparty_pname_total_temp_%(statdatenum)s
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.gameparty_pname_total_temp3_%(statdatenum)s;
        create table work.gameparty_pname_total_temp3_%(statdatenum)s as
        select
                b.fgamefsk,
                b.fplatformfsk,
                b.fhallfsk,
                b.fterminaltypefsk,
                b.fversionfsk,
                coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                coalesce(a.fpname,'%(null_str_report)s') fpname,
                a.fuid,
                b.hallmode,
                a.fbpid
            from stage.user_bankrupt_stg a
            join dim.bpid_map b
              on a.fbpid = b.fbpid
       left join analysis.marketing_channel_pkg_info mcp
              on a.fchannel_code = mcp.fid
           where a.dt = "%(statdate)s"
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """ --破产用户数
            select   fgamefsk,
                     coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                     coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                     coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                     coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                     coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                     coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                     coalesce(ut.fpname, '%(null_str_group_rule)s') fpname,

                     0 fusernum        , --玩牌人次
                     0 fpartynum       , --牌局数
                     0 fcharge         , --每局费用

                     0 fplayusernum    , --玩牌人数
                     0 fregplayusernum , --当天注册用户玩牌人次
                     0 factplayusernum , --当天活跃用户玩牌人次
                     0 fpaypartynum    , --付费用户且玩牌场次
                     0 fpayusernum     ,  --付费用户且玩牌用户数

                     count(distinct ut.fuid ) as  fbankruptusercnt ,  --当天破产用户数
                     0 fbankruptuserradio  , --破产率
                     0 frelievecnt     , --当天领取救济用户数
                     0 frelieveradio,    --救济率

                     0 fuserprofit,
                     0 fuserloss,
                     0 fpayplaynum,
                     0 fpayusercharge,
                     0 fpayuserprofit,
                     0 fpayuserloss
            from work.gameparty_pname_total_temp3_%(statdatenum)s ut
           where ut.hallmode=%(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fpname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        insert into table work.gameparty_pname_total_temp_%(statdatenum)s
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.gameparty_pname_total_temp4_%(statdatenum)s;
        create table work.gameparty_pname_total_temp4_%(statdatenum)s as
             select
                    b.fgamefsk,
                    b.fplatformfsk,
                    b.fhallfsk,
                    b.fterminaltypefsk,
                    b.fversionfsk,
                    coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(a.fpname,'%(null_str_report)s') fpname,
                    a.fuid,
                    b.hallmode,
                    a.fbpid
                from stage.user_bankrupt_relieve_stg a
                join dim.bpid_map b
                  on a.fbpid = b.fbpid
           left join analysis.marketing_channel_pkg_info mcp
                  on a.fchannel_code = mcp.fid
               where a.dt = "%(statdate)s"
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """ --破产领取救济用户数
        select   fgamefsk,
                 coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                 coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                 coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                 coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                 coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                 coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                 coalesce(ut.fpname, '%(null_str_group_rule)s') fpname,
                 0 fusernum        , --玩牌人次
                 0 fpartynum       , --牌局数
                 0 fcharge         , --每局费用

                 0 fplayusernum    , --玩牌人数
                 0 fregplayusernum , --当天注册用户玩牌人次
                 0 factplayusernum , --当天活跃用户玩牌人次
                 0 fpaypartynum    , --付费用户且玩牌场次
                 0 fpayusernum     ,  --付费用户且玩牌用户数

                 0 fbankruptusercnt    ,  --当天破产用户数
                 0 fbankruptuserradio  , --破产率
                 count(DISTINCT ut.fuid ) frelievecnt     , --当天领取救济用户数
                 0 frelieveradio,    --救济率

                 0 fuserprofit,
                 0 fuserloss,
                 0 fpayplaynum,
                 0 fpayusercharge,
                 0 fpayuserprofit,
                 0 fpayuserloss
            from work.gameparty_pname_total_temp4_%(statdatenum)s ut
           where ut.hallmode=%(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fpname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        insert into table work.gameparty_pname_total_temp_%(statdatenum)s
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table dcnew.gameparty_pname_total partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code fchannelcode,

            fpname,
            sum(fusernum),
            sum(fpartynum),
            sum(fcharge),

            sum(fplayusernum),
            sum(fregplayusernum),
            sum(factplayusernum),
            sum(fpaypartynum),
            sum(fpayusernum),

            sum(fbankruptusercnt),                                                                                              --当天破产用户数
            0 fbankruptuserradio,   --破产率
            sum(frelievecnt),                                                                                               --当天领取救济用户数
            0 frelieveradio,       --救济率

            sum(fuserprofit),
            sum(fuserloss),
            sum(fpayplaynum),
            sum(fpayusercharge),
            sum(fpayuserprofit),
            sum(fpayuserloss)

        from work.gameparty_pname_total_temp_%(statdatenum)s
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code, fpname
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        # 统计完清理掉临时表
        hql = """
          drop table if exists work.gameparty_pname_total_temp_%(statdatenum)s;
          drop table if exists work.gameparty_pname_total_temp2_%(statdatenum)s;
          drop table if exists work.gameparty_pname_total_temp3_%(statdatenum)s;
          drop table if exists work.gameparty_pname_total_temp4_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


# 生成统计实例
a = agg_gameparty_pname_total(sys.argv[1:])
a()
