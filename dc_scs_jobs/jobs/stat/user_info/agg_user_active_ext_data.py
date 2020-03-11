#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_active_ext_data(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """
        create table if not exists analysis.user_active_ext_fct
        (
            fdate             date,
            fgamefsk          bigint,
            fplatformfsk      bigint,
            fversionfsk       bigint,
            fterminalfsk      bigint,
            fgcusernum        bigint,
            fgccbactcnt       bigint,
            ffullactcnt       bigint,
            floginusernum     bigint,
            floginnum         bigint,
            f7dloginusernum   bigint,
            f7dloginnum       bigint,
            f30dloginusernum  bigint,
            f30dloginnum      bigint
        )
        partitioned by (dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        query["num_begin"] = query["ld_daybegin"].replace('-', '')

        hql = """
        use stage;
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.user_active_ext_fct
        partition(dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            count(distinct case when is_gcoin = 1 then fuid end) gcuseract,
            count(distinct case when is_gcoin = 1 and is_login = 0 then fuid end) gcact,
            count(distinct case when is_act = 1 then fuid end) fullact,
            0 floginusernum,
            0 floginnum,
            0 f7dloginusernum,
            0 f7dloginnum,
            0 f30dloginusernum,
            0 f30dloginnum
        from 
        (
            select fbpid, fuid,
                max(is_act) is_act,
                max(is_gcoin) is_gcoin,
                max(is_login) is_login
            from 
            (
                select fbpid, fuid, 1 is_act, 0 is_gcoin, 0 is_login
                from active_user_mid
                where dt = '%(ld_daybegin)s'
                
                union all
                
                select distinct fbpid, fuid, 0 is_act, 1 is_gcoin, 0 is_login
                from pb_gamecoins_stream_stg
                where dt = '%(ld_daybegin)s'
                
                union all
                
                select fbpid, fuid, 0 is_act, 0 is_gcoin, 1 is_login
                from user_login_stg
                where dt = '%(ld_daybegin)s'
            ) t
            group by fbpid, fuid
        ) a
        join analysis.bpid_platform_game_ver_map b
            on a.fbpid = b.fbpid
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        set mapreduce.reduce.shuffle.memory.limit.percent=0.1;
        
        insert overwrite table analysis.user_active_ext_fct
        partition(dt = '%(ld_daybegin)s')
        select fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            max(fgcusernum) fgcusernum,
            max(fgccbactcnt) fgccbactcnt,
            max(ffullactcnt) ffullactcnt,
            max(floginusernum) floginusernum,
            max(floginnum) floginnum,
            max(f7dloginusernum) f7dloginusernum,
            max(f7dloginnum) f7dloginnum,
            max(f30dloginusernum) f30dloginusernum,
            max(f30dloginnum) f30dloginnum 
        from 
        (
            select  fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fgcusernum,
                fgccbactcnt,
                ffullactcnt,
                floginusernum,
                floginnum,
                f7dloginusernum,
                f7dloginnum,
                f30dloginusernum,
                f30dloginnum 
            from analysis.user_active_ext_fct 
            where dt = "%(ld_daybegin)s"
            and ffullactcnt > 0
        
            union all
        
            select '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                0 fgcusernum,
                0 fgccbactcnt,
                0 ffullactcnt,
                count(distinct if(a.dt >= '%(ld_daybegin)s', a.fuid, null)) floginusernum,
                sum(if(a.dt >= '%(ld_daybegin)s', a.loginusernum, null)) floginnum,
                
                count(distinct if(a.dt >= '%(ld_6dayago)s', a.fuid, null)) f7dloginusernum,
                sum(if(a.dt >= '%(ld_6dayago)s', a.loginusernum, null)) f7dloginnum,
                
                count(distinct a.fuid) f30dloginusernum,
                sum(a.loginusernum) f30dloginnum
            from 
            (
                select dt, fbpid, fuid, count(1) loginusernum
                from stage.user_login_stg
                where dt >= '%(ld_29dayago)s' and dt < '%(ld_dayend)s'
                group by dt, fbpid, fuid
            ) a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        ) a 
        group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
#生成统计实例
a = agg_user_active_ext_data(statDate)
a()
