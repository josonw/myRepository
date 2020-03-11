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


class load_user_match_party_stream(BaseStat):
    def create_tab(self):
        hql = """
        create table if not exists dim.user_match_party_stream
        (
            fbpid                  varchar(50),
            flts_at                string,
            fuid                   bigint,
            ftbl_id                varchar(64),
            finning_id             varchar(64),
            fpalyer_cnt            smallint,
            fs_timer               string,
            fe_timer               string,
            fgamecoins             bigint,
            fuser_gcoins           bigint,
            fcharge                decimal(20,2),
            fsubname               varchar(100),
            fpname                 varchar(100),
            fblind_1               bigint,
            fblind_2               bigint,
            flimit_gcoins          bigint,
            fhas_fold              tinyint,
            fhas_open              tinyint,
            frobots_num            tinyint,
            ftrustee_num           smallint,
            fis_king               tinyint,
            fserv_charge           bigint,
            fcard_type             varchar(50),
            fversion_info          varchar(50),
            fchannel_code          bigint,
            fintegral_val          bigint,
            fintegral_balance      bigint,
            fis_weedout            tinyint,
            fis_bankrupt           tinyint,
            fis_end                tinyint,
            fmatch_id              varchar(100),
            fms_time               string,
            fme_time               string,
            flevel                 bigint,
            fgsubname              varchar(128),
            ffirst_play            tinyint,
            fgame_id               int,
            ffirst_play_sub        tinyint,
            ffirst_match           tinyint,
            fcoins_type            int,
            fparty_type            varchar(100),
            ffirst_play_gsub       int
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

    def stat(self):
        alias_dic = {'bpid_tbl_alias':'mcp.','src_tbl_alias':'ug.', 'const_alias':''}
        query = {'statdate':self.stat_date,
                'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],
                'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
                }

        query.update(sql_const.const_dict())
        query.update(PublicFunc.date_define(self.stat_date))

        hql ="""
            insert overwrite table dim.user_match_party_stream
            partition( dt="%(statdate)s" )
            select /*+ MAPJOIN(mcp) */
                fbpid,
                flts_at,
                fuid,
                ftbl_id,
                finning_id,
                fpalyer_cnt,
                fs_timer,
                fe_timer,
                fgamecoins,
                fuser_gcoins,
                fcharge,
                coalesce(ug.fsubname,'%(null_str_report)s') fsubname,
                coalesce(ug.fpname,'%(null_str_report)s') fpname,
                fblind_1,
                fblind_2,
                flimit_gcoins,
                fhas_fold,
                fhas_open,
                frobots_num,
                ftrustee_num,
                fis_king,
                fserv_charge,
                fcard_type,
                fversion_info,
                coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                fintegral_val,
                fintegral_balance,
                fis_weedout,
                fis_bankrupt,
                fis_end,
                fmatch_id,
                fms_time,
                fme_time,
                flevel,
                fgsubname,
                ffirst_play,
                coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                ffirst_play_sub,
                ffirst_match,
                fcoins_type,
                coalesce(ug.fparty_type, '%(null_str_report)s') fparty_type,
                ffirst_play_gsub
            from stage.user_gameparty_stg ug
            left join analysis.marketing_channel_pkg_info mcp
              on ug.fchannel_code = mcp.fid
           where ug.dt = "%(statdate)s" and  coalesce(ug.fmatch_id,'0')<>'0'
        """% query
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
a = load_user_match_party_stream(statDate, eid)
a()
