# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_data(BaseStat):
    def create_tab(self):

        hql = """
        use analysis;
        create table if not exists analysis.user_gameparty_game_type_fct
        (
          fdate               date,
          fgamefsk            bigint,          --游戏编号
          fplatformfsk        bigint,          --平台编号
          fversionfsk         bigint,          --版本编号
          fterminalfsk        bigint,          --终端编号

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


        hql = """    -- 新老牌局整合 PS:直接去掉了老牌局数据
        drop table if exists stage.agg_gameparty_data_type_2_%(num_begin)s;
        create table stage.agg_gameparty_data_type_2_%(num_begin)s as
          select    fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,

                    fpname,
                    0 fusernum,
                    count(distinct concat_ws('0', finning_id, ftbl_id) ) fpartynum,
                    round(sum(a.fcharge), 4) fcharge,

                    0 fplayusernum,
                    0 fregplayusernum,
                    0 factplayusernum,
                    0 fpaypartynum,
                    0 fpayusernum,

                    0 fbankruptusercnt    ,  --当天破产用户数
                    0 fbankruptuserradio  , --破产率
                    0 frelievecnt     , --当天领取救济用户数
                    0 frelieveradio,    --救济率

                    sum(case when fgamecoins>=0 then fgamecoins else 0 end) fuserprofit,
                    sum(case when fgamecoins< 0 then fgamecoins else 0 end) fuserloss,
                    0 fpayplaynum,
                    0 fpayusercharge,
                    0 fpayuserprofit,
                    0 fpayuserloss
                from stage.user_gameparty_stg a
                      join analysis.bpid_platform_game_ver_map bpm
                        on a.fbpid = bpm.fbpid
                        where a.dt = "%(ld_begin)s"
                    group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,fpname
        union all
            select fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,

                 fpname,
                 sum(ut.fplaycnt) fusernum,
                 0 fpartynum,
                 0 fcharge,

                 count(distinct ut.fuid) fplayusernum,
                 0 fregplayusernum,
                 0 factplayusernum,
                 count(distinct case when ispayuser=1 then concat_ws('0', finning_id, ftbl_id) else null end ) fpaypartynum,  --付费用户牌局数
                 count(distinct case when ispayuser=1 then ut.fuid else null end ) fpayusernum,   --付费用户且玩牌用户数

                 0 fbankruptusercnt    ,  --当天破产用户数
                 0 fbankruptuserradio  , --破产率
                 0 frelievecnt     , --当天领取救济用户数
                 0 frelieveradio,    --救济率

                 0 fuserprofit,
                 0 fuserloss,
                 sum(ut.fplaycnt*ispayuser) fpayplaynum,    --付费用户玩牌人次
                 round(sum(ut.fcharge*ispayuser), 4) fpayusercharge,   --付费用户每局费用
                 sum(ut.fpayuserprofit*ispayuser) fpayuserprofit,  --付费用户玩牌盈利
                 sum(ut.fpayuserloss*ispayuser) fpayuserloss   --付费用户玩牌亏损

             from (
                select a.fbpid, a.fpname, a.fuid, a.finning_id, a.ftbl_id, sum(a.fcharge) fcharge, count(1) fplaycnt,
                       sum(case when fgamecoins>=0 then fgamecoins else 0 end) fpayuserprofit,
                       sum(case when fgamecoins <0 then abs(fgamecoins) else 0 end) fpayuserloss,
                       case when c.fplatform_uid is null then 0 else 1 end ispayuser
                        from stage.user_gameparty_stg a

                        left join (SELECT fbpid, max(fplatform_uid) fplatform_uid, fuid
                                   FROM
                                      (SELECT fbpid, fplatform_uid, fuid,
                                                 row_number() over(partition BY fbpid,fplatform_uid) AS flag
                                          FROM stage.user_pay_info where dt='%(ld_begin)s' ) AS foo
                                       WHERE flag =1
                                        group by fbpid, fuid
                                    ) c
                        on a.fbpid=c.fbpid and a.fuid=c.fuid
                        where a.dt='%(ld_begin)s'
                    group by a.fbpid, a.fpname, c.fplatform_uid, a.fuid, a.finning_id, a.ftbl_id
             ) ut
             join analysis.bpid_platform_game_ver_map bpm
               on ut.fbpid = bpm.fbpid
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,fpname
        ;
        """ % self.hql_dict
        hql_list.append(hql)


        #破产用户数，
        hql = """ --破产用户数
            insert into table stage.agg_gameparty_data_type_2_%(num_begin)s
            select /*+ STREAMTABLE(user_gameparty_stg)*/
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     fterminalfsk,

                     a.fpname,
                     0 fusernum        , --玩牌人次
                     0 fpartynum       , --牌局数
                     0 fcharge         , --每局费用

                     0 fplayusernum    , --玩牌人数
                     0 fregplayusernum , --当天注册用户玩牌人次
                     0 factplayusernum , --当天活跃用户玩牌人次
                     0 fpaypartynum    , --付费用户且玩牌场次
                     0 fpayusernum     ,  --付费用户且玩牌用户数

                     count(distinct a.fuid ) as  fbankruptusercnt ,  --当天破产用户数
                     0 fbankruptuserradio  , --破产率
                     0 frelievecnt     , --当天领取救济用户数
                     0 frelieveradio,    --救济率

                     0 fuserprofit,
                     0 fuserloss,
                     0 fpayplaynum,
                     0 fpayusercharge,
                     0 fpayuserprofit,
                     0 fpayuserloss

            from stage.user_bankrupt_stg a
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
                where a.dt = "%(ld_begin)s"
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk , a.fpname ;
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """ --破产领取救济用户数
        insert into table stage.agg_gameparty_data_type_2_%(num_begin)s
        select   fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,

                 a.fpname,
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
                 count(DISTINCT a.fuid ) frelievecnt     , --当天领取救济用户数
                 0 frelieveradio,    --救济率

                 0 fuserprofit,
                 0 fuserloss,
                 0 fpayplaynum,
                 0 fpayusercharge,
                 0 fpayuserprofit,
                 0 fpayuserloss
        from stage.user_bankrupt_relieve_stg a
        join analysis.bpid_platform_game_ver_map bpm
         on a.fbpid = bpm.fbpid
         where a.dt = "%(ld_begin)s"
        group by fplatformfsk , fgamefsk, fversionfsk, fterminalfsk ,a.fpname ;
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_gameparty_game_type_fct partition( dt="%(ld_begin)s" )
            select "%(ld_begin)s" fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,

            coalesce(fpname,'未知') fpname,
            sum(fusernum),
            sum(fpartynum),
            sum(fcharge),

            sum(fplayusernum),
            sum(fregplayusernum),
            sum(factplayusernum),
            sum(fpaypartynum),
            sum(fpayusernum),

            sum(fbankruptusercnt),                                                                                              --当天破产用户数
            case when sum(fplayusernum) = 0 then 0 else round(sum(fbankruptusercnt)/sum(fplayusernum),4) end  fbankruptuserradio,   --破产率
            sum(frelievecnt),                                                                                               --当天领取救济用户数
            case when sum(fbankruptusercnt) = 0 then 0 else round(sum(frelievecnt)/sum(fbankruptusercnt),4) end frelieveradio,       --救济率

            sum(fuserprofit),
            sum(fuserloss),
            sum(fpayplaynum),
            sum(fpayusercharge),
            sum(fpayuserprofit),
            sum(fpayuserloss)

        from stage.agg_gameparty_data_type_2_%(num_begin)s
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,coalesce(fpname,'未知')
        """ % self.hql_dict
        hql_list.append(hql)

        # 统计完清理掉临时表
        hql = """
          drop table if exists stage.agg_gameparty_data_type_2_%(num_begin)s;
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
a = agg_gameparty_data(statDate, eid)
a()
