#!/user/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_user_vip_fct_new(BaseStat):
    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_vip_fct_new
        (
            fdate    date    ,
            fgamefsk    bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk bigint  ,
            fvip_type   int  ,
            fvip_level  int  ,
            flevel  int  ,
            fdaucnt bigint  ,
            fdsucnt bigint  ,
            fcurcnt bigint  ,
            fduecnt bigint  ,
            fpartyn_vip bigint  ,
            fpartyuser_vip  bigint  ,
            frenewalsnum    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_vip_fct_new'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):

        query = { 'statdate':statDate, "num_begin": statDate.replace('-', ''),
            'ld_1daylater':PublicFunc.add_days(statDate, 1) }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """--临时表统计当天有哪些用户是vip
        drop table if exists stage.agg_user_vip_fct_new_2_%(num_begin)s;

        create table stage.agg_user_vip_fct_new_2_%(num_begin)s as
        select   fbpid, fuid, fvip_at, fdue_at, fvip_type, fplatform_uid, fvip_level,
                 flevel, foper_type, foper_way,fpay_uid, fpay_way,fpay_info,
                 fip, fmoney, fdays, ffirst_at, flast_due_at, fversion_info, fchannel_code
        from (select fbpid,fuid, fvip_at, fdue_at, coalesce( fvip_type,-1) fvip_type, fplatform_uid, coalesce( fvip_level,-1) fvip_level, coalesce( flevel,-1) flevel, foper_type,
                     foper_way, fpay_uid, fpay_way, fpay_info, fip, fmoney, fdays,
                     ffirst_at, flast_due_at, fversion_info, fchannel_code,
                      row_number() over(partition by fbpid, fuid order by fvip_at,fdue_at, coalesce( fvip_type,-1),  coalesce( fvip_level,-1), coalesce( flevel,-1) desc) as flag
                from stage.user_vip_stg a
               where fvip_at < "%(ld_1daylater)s"
                 and fdue_at > "%(ld_1daylater)s"
                     ) aa
         where flag = 1""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """--vip活跃数和vip当前数,vip玩牌数和玩牌人数
        drop table if exists stage.agg_user_vip_fct_new_1_%(num_begin)s;

        create table stage.agg_user_vip_fct_new_1_%(num_begin)s as
        select "%(statdate)s" fdate,
             d.fgamefsk,
             d.fplatformfsk,
             d.fversionfsk,
             coalesce(a.fvip_type,-1) fvip_type,
             coalesce(a.fvip_level,-1) fvip_level,
             coalesce(a.flevel,-1) flevel,
             coalesce(count(distinct(case when is_act is not null then a.fuid end)), 0) fdaucnt,
             0 fdsucnt,
             coalesce(count(distinct a.fuid), 0) fcurcnt,
             0 fduecnt,
             coalesce(sum(case when is_party is not null then fparty_num end), 0) fpartyn_vip,
             coalesce(count(distinct(case when is_party is not null then a.fuid end)), 0) fpartyuser_vip ,
             0 frenewalsnum
             from (select a.fbpid,
                     a.fvip_type,
                     a.fvip_level,
                     a.flevel,
                     a.fuid,
                     fparty_num,
                     b.fuid       is_act,
                     c.fuid       is_party
                from stage.agg_user_vip_fct_new_2_%(num_begin)s a
                left outer join stage.active_user_mid b
                  on a.fbpid = b.fbpid
                 and a.fuid = b.fuid
                 and b.dt = "%(statdate)s"
                left outer join stage.user_gameparty_info_mid c
                  on a.fbpid = c.fbpid
                 and a.fuid = c.fuid
                 and c.dt = "%(statdate)s"
                 ) a
        join analysis.bpid_platform_game_ver_map d
          on a.fbpid = d.fbpid
       group by d.fgamefsk,
                d.fplatformfsk,
                d.fversionfsk,
                coalesce(a.fvip_type,-1),
                coalesce(a.fvip_level,-1),
                coalesce(a.flevel,-1)""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """--vip新增（开通）数和续费数（通过foper_type判断）
        insert into table stage.agg_user_vip_fct_new_1_%(num_begin)s

        select "%(statdate)s" fdate,
                  c.fgamefsk,
                  c.fplatformfsk,
                  c.fversionfsk,
                  coalesce(a.fvip_type,-1) fvip_type,
                  coalesce(a.fvip_level,-1) fvip_level,
                  coalesce(a.flevel,-1) flevel,
                  0 fdaucnt,
                coalesce(count(distinct(case when foper_type = '1' then a.fuid end)), 0) fdsucnt,
                0 fcurcnt,
                0 fduecnt,
                0 fpartyn_vip,
                0 fpartyuser_vip,
                coalesce(count(distinct(case when foper_type = '2' then a.fuid end)), 0) frenewalsnum
                from stage.user_vip_stg a
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            where a.dt = "%(statdate)s"
            group by c.fgamefsk,
                     c.fplatformfsk,
                     c.fversionfsk,
                     coalesce(a.fvip_type,-1),
                     coalesce(a.fvip_level,-1),
                     coalesce(a.flevel,-1)""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """--当天过期vip用户数（扣除当天过期又续费的用户）
        insert into table stage.agg_user_vip_fct_new_1_%(num_begin)s

        select "%(statdate)s" fdate,
                  fgamefsk,
                  fplatformfsk,
                  fversionfsk,
                  coalesce(a.fvip_type,-1) fvip_type,
                  coalesce(a.fvip_level,-1) fvip_level,
                  coalesce(a.flevel,-1) flevel,
                  0 fdaucnt,
                    0 fdsucnt,
                    0 fcurcnt,
                    coalesce(count(distinct a.fuid), 0) fduecnt,
                    0 fpartyn_vip,
                    0 fpartyuser_vip,
                    0 frenewalsnum
             from (select m.fbpid, m.fuid, m.fvip_type, m.fvip_level, m.flevel
                      from (select fbpid, fuid, fvip_type, fvip_level, flevel
                              from stage.user_vip_stg
                             where fdue_at >= "%(statdate)s"
                               and fdue_at < "%(ld_1daylater)s"
                               ) m
                      left outer join (select fbpid, fuid, fvip_type, fvip_level, flevel
                                         from stage.user_vip_stg
                                        where foper_type = '2'
                                          and flast_due_at >= "%(statdate)s"
                                          and flast_due_at < "%(ld_1daylater)s") n
                        on m.fbpid=n.fbpid and m.fuid=n.fuid and m.fvip_type=n.fvip_type
                        and m.fvip_level=n.fvip_level and m.flevel=n.flevel
                     where n.fbpid is null) a
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            group by fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     coalesce(a.fvip_type,-1),
                     coalesce(a.fvip_level,-1),
                     coalesce(a.flevel,-1)""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """   --
        insert overwrite table analysis.user_vip_fct_new
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate,
                    fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fvip_type,
                    fvip_level,
                    flevel,
                    max(fdaucnt) fdaucnt,
                    max(fdsucnt) fdsucnt,
                    max(fcurcnt) fcurcnt,
                    max(fduecnt) fduecnt,
                    max(fpartyn_vip) fpartyn_vip,
                    max(fpartyuser_vip) fpartyuser_vip,
                    max(frenewalsnum) frenewalsnum
            from stage.agg_user_vip_fct_new_1_%(num_begin)s

            group by fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fvip_type,
                    fvip_level,
                    flevel
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_user_vip_fct_new_1_%(num_begin)s;

        drop table if exists stage.agg_user_vip_fct_new_2_%(num_begin)s;
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
a = agg_user_vip_fct_new(statDate, eid)
a()