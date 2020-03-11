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


class load_gameparty_info(BaseStatModel):

#将原过程拆分为三个，分别运行，减少单个脚本资源占用，提高整体速度
#
#后置依赖:load_gameparty_stream ， load_gameparty
#本脚本生成 dim.user_gameparty_stream

    """生成三个中间表
    dim.user_gameparty_stream （gameparty源表） + 7个粒度 + hallmode（ load_gameparty_info ）
    dim.gameparty_stream （桌子id为粒度）+ 7个粒度 + hallmode（ load_gameparty_stream ）
    dim.user_gameparty 以（uid，pname，subname）+ 7个粒度 + hallmode （ load_gameparty ）
     """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_gameparty_stream
        (
            fbpid varchar(50),
            fgamefsk  bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            hallmode smallint,
            fgame_id bigint,
            fchannel_code bigint,
            flts_at string,
            fuid    bigint,
            ftbl_id varchar(64),
            finning_id  varchar(64),
            fpalyer_cnt smallint,
            fs_timer    string,
            fe_timer    string,
            fgamecoins  bigint,
            fuser_gcoins    bigint,
            fcharge decimal(20,6),
            fsubname    varchar(100),
            fpname  varchar(100),
            fblind_1    bigint,
            fblind_2    bigint,
            flimit_gcoins   bigint,
            fhas_fold   tinyint,
            fhas_open   tinyint,
            frobots_num tinyint,
            ftrustee_num    smallint,
            fis_king    tinyint,
            fserv_charge    bigint,
            fcard_type  varchar(50),
            fversion_info   varchar(50),
            fintegral_val   bigint,
            fintegral_balance   bigint,
            fis_weedout tinyint,
            fis_bankrupt    tinyint,
            fis_end tinyint,
            fmatch_id   varchar(100),
            fms_time    string,
            fme_time    string,
            fvip_type   varchar(100),
            fvip_level  bigint,
            flevel  bigint,
            fgsubname   varchar(128),
            ftbuymin    int,
            ftbuymax    int,
            fprechip    int,
            ffirst_play tinyint,
            ffirst_play_sub tinyint,
            ffirst_match    tinyint,
            fcoins_type int ,
            fparty_type varchar(100),
            ffirst_play_gsub    int
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


    def stat(self):

        hql ="""
            insert overwrite table dim.user_gameparty_stream
            partition(dt='%(statdate)s')
            select /*+ MAPJOIN(b,mcp) */
                    a.fbpid,
                    b.fgamefsk,
                    b.fplatformfsk,
                    b.fhallfsk,
                    b.fterminaltypefsk,
                    b.fversionfsk,
                    b.hallmode,
                    coalesce(a.fgame_id, cast(0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
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
                    coalesce(fsubname,'%(null_str_report)s') fsubname,
                    coalesce(fpname,'%(null_str_report)s') fpname,
                    coalesce(fblind_1,%(null_int_report)d) fblind_1,
                    fblind_2,
                    flimit_gcoins,
                    fhas_fold,
                    fhas_open,
                    frobots_num,
                    ftrustee_num,
                    fis_king,
                    fserv_charge,
                    coalesce(fcard_type,'%(null_str_report)s')  fcard_type,
                    fversion_info,
                    fintegral_val,
                    fintegral_balance,
                    fis_weedout,
                    fis_bankrupt,
                    fis_end,
                    fmatch_id,
                    fms_time,
                    fme_time,
                    fvip_type,
                    fvip_level,
                    flevel,
                    coalesce(fgsubname, '%(null_str_report)s')  fgsubname,
                    coalesce(ftbuymin, %(null_int_report)d)  ftbuymin,
                    coalesce(ftbuymax, %(null_int_report)d)  ftbuymax,
                    coalesce(fprechip, %(null_int_report)d)  fprechip,
                    ffirst_play,
                    ffirst_play_sub,
                    ffirst_match,
                    fcoins_type,
                    coalesce(fparty_type, '%(null_str_report)s')  fparty_type,
                    ffirst_play_gsub
                from stage.user_gameparty_stg a
                join dim.bpid_map b
                  on a.fbpid=b.fbpid
                left join analysis.marketing_channel_pkg_info mcp
                on a.fchannel_code = mcp.fid
                where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res



#生成统计实例
a = load_gameparty_info(sys.argv[1:])
a()
