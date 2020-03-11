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


class agg_match_user_party_time_dis(BaseStatModel):
    def create_tab(self):

        hql = """create table if not exists dcnew.match_user_party_time_dis
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
            bs_playt                   int, --时长分布
            fpartyucnt                 bigint       --牌局参赛人数

            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fpname', 'fname', 'fsubname','bs_playt'],
                        'groups':[[1, 1, 1, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0: return res

        hql = """
        drop table if exists work.match_user_party_time_dis_tmp_b_%(statdatenum)s;
        create table work.match_user_party_time_dis_tmp_b_%(statdatenum)s as
           select c.fgamefsk, c.fplatformfsk, c.fhallfsk, c.fterminaltypefsk, c.fversionfsk, c.hallmode,
                  coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                  coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                  coalesce(ug.fpname,'%(null_str_report)s') fpname,
                  coalesce(ug.fsubname,'%(null_str_report)s') fname,
                  coalesce(ug.fgsubname,'%(null_str_report)s') fsubname,
                  ug.fuid,
                  sum(case when int((unix_timestamp(fe_timer)-unix_timestamp(fs_timer))/60)>=0 and
                           int((unix_timestamp(fe_timer)-unix_timestamp(fs_timer))/60)<=60 then int((unix_timestamp(fe_timer)-unix_timestamp(fs_timer))/60)
                       else 61 end) bs_playt,
                  ug.fmatch_id
             from stage.user_gameparty_stg ug
             left join analysis.marketing_channel_pkg_info mcp
               on ug.fchannel_code = mcp.fid
             join dim.bpid_map c
               on ug.fbpid=c.fbpid
            where ug.dt='%(statdate)s'
              and coalesce(fmatch_id,'0')<>'0'
            group by c.fgamefsk, c.fplatformfsk, c.fhallfsk, c.fterminaltypefsk, c.fversionfsk, c.hallmode,
                    coalesce(ug.fgame_id,cast (0 as bigint)),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(ug.fpname,'%(null_str_report)s'),
                    coalesce(ug.fsubname,'%(null_str_report)s'),
                    coalesce(ug.fgsubname,'%(null_str_report)s'),
                    ug.fuid,
                    ug.fmatch_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(case when fgamefsk = 4132314431 and fpname<>'%(null_str_report)s' then '比赛场' else fpname end,'%(null_str_group_rule)s') fpname,
                       coalesce(fname,'%(null_str_group_rule)s') fname,
                       coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                       bs_playt,
                       count(fuid) fpartyucnt
                  from work.match_user_party_time_dis_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fpname,fname,fsubname,bs_playt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.match_user_party_time_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.match_user_party_time_dis_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

#生成统计实例
a = agg_match_user_party_time_dis(sys.argv[1:])
a()
