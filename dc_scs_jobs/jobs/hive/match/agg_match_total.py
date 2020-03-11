#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

class agg_match_total(BaseStatModel):
    """比赛场数据概览
    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.match_total
            (
            fdate                      date,
            fgamefsk                   bigint,
            fplatformfsk               bigint,
            fhallfsk                   bigint,
            fsubgamefsk                bigint,
            fterminaltypefsk           bigint,
            fversionfsk                bigint,
            fchannelcode               bigint,

            fpname                     varchar(100), --比赛场名称 比如“斗地主比赛场”
            fname                      varchar(100), --二级赛名称 比如“话费赛”
            fsubname                   varchar(50),  --三级赛名称 比如“XX赛(10点-12点)”
            fmode                      varchar(100), --报名模式，比如有偿报名、免费报名

            fapplyucnt                 bigint,  --牌局报名人数
            fapplycnt                  bigint,  --牌局报名人次
            fpartyucnt                 bigint,  --牌局参赛人数
            fpartycnt                  bigint,  --牌局参赛人次
            fmatchnum                  bigint,  --牌局开赛次数
            fwinusernum                bigint,  --获奖人数
            fautoquitnum               bigint,  --自动退赛人数
            fsysquitnum                bigint   --系统退赛人数
        )
        partitioned by(dt date)
        location '/dw/dcnew/match_total'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        extend_group = {
            'fields':['fpname', 'fname', 'fsubname','fmode'],
            'groups':[ [1, 1, 1, 1],
                       [1, 1, 0, 1],
                       [1, 0, 0, 1],
                       [0, 0, 0, 1],
                       [1, 1, 1, 0],
                       [1, 1, 0, 0],
                       [1, 0, 0, 0],
                       [0, 0, 0, 0]
                       ]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;
                              set hive.groupby.skewindata=true;
                              """)
        if res != 0: return res


        hql = """ -- 取出所有比赛相关数据
        drop table if exists work.match_total_temp_%(statdatenum)s;
        create table work.match_total_temp_%(statdatenum)s
        as
        select c.fgamefsk,
               c.fplatformfsk,
               c.fhallfsk,
               c.fterminaltypefsk,
               c.fversionfsk,
               c.hallmode,
               a.fgame_id,
               a.fchannel_code,
               coalesce(case when c.fgamefsk = 4132314431 and a.fpname<>'-13658' then '比赛场' else a.fpname end,'-21379') fpname,
               a.fname,
               a.fsubname,
               a.fuid,
               a.fmatch_id,
               max(a.fcause) fcause,
               coalesce(max(a.fmode),'%(null_str_report)s') fmode,
               max(case when fio_type=1 then fio_type else 0 end) fio_type,
               max(fapplycnt) fapplycnt,
               max(fpartycnt) fpartycnt
        from
        (
            select a.fbpid, a.fgame_id, a.fchannel_code, a.fpname, a.fname, a.fsubname,
                  a.fuid, a.fmatch_id, max(a.fmode) over(partition by a.fmatch_id,a.fuid ) fmode, a.fcause,
            fio_type,fapplycnt,fpartycnt
            from
             (select fbpid,
                    coalesce(jg.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(jg.fpname,'%(null_str_report)s') fpname,
                    coalesce(jg.fname,'%(null_str_report)s') fname,
                    coalesce(jg.fsubname,'%(null_str_report)s') fsubname,
                    jg.fmode,jg.fuid,jg.fmatch_id,0 fcause, 0 fio_type, 1 flag,
                    1 fapplycnt, 0 fpartycnt
                from stage.join_gameparty_stg jg
                left join analysis.marketing_channel_pkg_info mcp
                on jg.fchannel_code = mcp.fid  where jg.dt='%(statdate)s' and  coalesce(fmatch_id,'0')<>'0'
                group by fbpid,
                    coalesce(jg.fgame_id,cast (0 as bigint)),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(jg.fpname,'%(null_str_report)s'),
                    coalesce(jg.fname,'%(null_str_report)s'),
                    coalesce(jg.fsubname,'%(null_str_report)s'),jg.fmode,jg.fuid,jg.fmatch_id

                union all

                select fbpid,
                    coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(ug.fpname,'%(null_str_report)s') fpname,
                    coalesce(ug.fsubname,'%(null_str_report)s') fname,
                    coalesce(ug.fgsubname,'%(null_str_report)s') fsubname,
                    null fmode,ug.fuid,ug.fmatch_id,0 fcause, 0 fio_type, 2 flag,
                    0 fapplycnt, 1 fpartycnt
                from stage.user_gameparty_stg ug
                left join analysis.marketing_channel_pkg_info mcp
                on ug.fchannel_code = mcp.fid  where ug.dt='%(statdate)s'  and  coalesce(fmatch_id,'0')<>'0'
                group by fbpid,
                    coalesce(ug.fgame_id,cast (0 as bigint)),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(ug.fpname,'%(null_str_report)s'),
                    coalesce(ug.fsubname,'%(null_str_report)s'),
                    coalesce(ug.fgsubname,'%(null_str_report)s'),ug.fuid,ug.fmatch_id

                union all

                select fbpid,
                    coalesce(qg.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(qg.fpname,'%(null_str_report)s') fpname,
                    coalesce(qg.fsubname,'%(null_str_report)s') fname,
                    coalesce(qg.fgsubname,'%(null_str_report)s') fsubname,
                    null fmode,fuid,qg.fmatch_id, fcause, 0 fio_type, 3 flag,
                    0 fapplycnt, 0 fpartycnt
                from stage.quit_gameparty_stg qg
                left join analysis.marketing_channel_pkg_info mcp
                on qg.fchannel_code = mcp.fid  where qg.dt='%(statdate)s' and  coalesce(fmatch_id,'0')<>'0'
                group by fbpid,
                    coalesce(qg.fgame_id,cast (0 as bigint)),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(qg.fpname,'%(null_str_report)s'),
                    coalesce(qg.fsubname,'%(null_str_report)s'),
                    coalesce(qg.fgsubname,'%(null_str_report)s'),qg.fuid,qg.fmatch_id,qg.fcause

                union all

                select fbpid,
                    coalesce(mg.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(mg.fpname,'%(null_str_report)s') fpname,
                    coalesce(mg.fsubname,'%(null_str_report)s') fname,
                    coalesce(mg.fgsubname,'%(null_str_report)s') fsubname,
                    null fmode,fuid,mg.fmatch_id,0 fcause, fio_type, 4 flag,
                    0 fapplycnt, 0 fpartycnt
                from stage.match_economy_stg mg
                left join analysis.marketing_channel_pkg_info mcp
                on mg.fchannel_code = mcp.fid  where mg.dt='%(statdate)s'  and  coalesce(fmatch_id,'0')<>'0'
                group by fbpid,
                    coalesce(mg.fgame_id,cast (0 as bigint)),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(mg.fpname,'%(null_str_report)s'),
                    coalesce(mg.fsubname,'%(null_str_report)s'),
                    coalesce(mg.fgsubname,'%(null_str_report)s'),mg.fuid,mg.fmatch_id,mg.fio_type
                ) a
            ) a
        join dim.bpid_map c
          on a.fbpid=c.fbpid
        group by c.fgamefsk,
               c.fplatformfsk,
               c.fhallfsk,
               c.fterminaltypefsk,
               c.fversionfsk,
               c.hallmode,
               fgame_id, fchannel_code, fpname, fname, fsubname, fuid, fmatch_id

            """

        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
          select '%(statdate)s' fdate,
                 fgamefsk,
                 coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                 coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                 coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                 coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                 coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                 coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,

                 --将地方棋牌的fpame全部改为比赛场，后期要修正过来
                 coalesce(fpname,'%(null_str_group_rule)s') fpname,
                 coalesce(fname,'%(null_str_group_rule)s') fname,
                 coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                 coalesce(fmode,'%(null_str_group_rule)s') fmode,

                 count(distinct case when fapplycnt > 0 then fuid else null end) fapplyucnt,
                 sum(fapplycnt) fapplycnt,
                 count(distinct case when fpartycnt > 0 then fuid else null end) fpartyucnt,
                 sum(fpartycnt) fpartycnt,
                 count(distinct case when fpartycnt > 0 then fmatch_id else null end) fmatchnum,
                 count(distinct case when fio_type = 1 then fuid else null end) fwinusernum,
                 count(distinct case when fcause = 1  then fuid else null end) fautoquitnum,
                 count(distinct case when fcause = 2  then fuid else null end) fsysquitnum
            from work.match_total_temp_%(statdatenum)s
           where hallmode=%(hallmode)s
           group by fgamefsk,
               fplatformfsk,
               fhallfsk,
               fterminaltypefsk,
               fversionfsk,
               hallmode,
               fgame_id, fchannel_code, fpname, fname, fsubname, fmode
            """

        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        insert overwrite table dcnew.match_total
        partition(dt = '%(statdate)s')
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        # 统计完清理掉临时表
        hql = """
        drop table if exists work.match_total_temp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_match_total(sys.argv[1:])
a()
