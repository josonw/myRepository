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


class load_push_user(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dim.push_user
        (
            fbpid                   varchar(50) ,
            fuid                    bigint      ,
            fgamefsk                bigint      ,
            fplatformfsk            bigint      ,
            fappid                  varchar(50) ,
            ftoken                  varchar(100),
            flts_at                 string      ,
            flogin_at               string      ,
            fsignup_at              string      ,
            fis_open                int         ,
            fversion_info           varchar(100),
            fuser_gamecoins         bigint      ,
            fm_dtype                varchar(100),
            fentrance_id            int         ,
            fis_paid                int         ,
            fvip_level              int         ,
            fgender                 int         ,
            fchannel_code           varchar(100),
            fparty_num              bigint      ,
            fpay_num                bigint      ,
            fparty_tnum             bigint      ,
            fpay_tnum               bigint
        )
        partitioned by (dt date)
        clustered by (fuid) sorted by (fbpid) into 256 buckets
        stored as orc
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容  """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        hql = """
        insert overwrite table dim.push_user
        partition(dt='%(statdate)s')
        select
            coalesce(a.fbpid, b.fbpid) fbpid,
            coalesce(a.fuid, b.fuid) fuid,
            max(b.fgamefsk) fgamefsk,
            max(b.fplatformfsk) fplatformfsk,
            max(coalesce(a.fappid, b.fappid)) fappid,
            max(coalesce(a.ftoken,b.ftoken)) ftoken,
            max(coalesce(a.flts_at,b.flts_at)) flts_at,
            max(coalesce(a.flogin_at, b.flogin_at)) flogin_at,
            max(coalesce(a.fsignup_at, b.fsignup_at)) fsignup_at,
            max(coalesce(a.fis_open, b.fis_open)) fis_open,
            max(coalesce(a.fversion_info, b.fversion_info)) fversion_info,
            max(coalesce(a.fuser_gamecoins, b.fuser_gamecoins)) fuser_gamecoins,
            max(coalesce(a.fm_dtype, b.fm_dtype)) fm_dtype,
            max(coalesce(a.fentrance_id, b.fentrance_id)) fentrance_id,
            coalesce(max(a.fis_paid), b.fis_paid) fis_paid,
            coalesce(max(a.fvip_level), b.fvip_level)) fvip_level,
            coalesce(max(a.fgender), b.fgender)) fgender,
            max(coalesce(a.fchannel_code, b.fchannel_code)) fchannel_code,
            sum(coalesce(a.fparty_num, b.fparty_num)) fparty_num,
            sum(coalesce(a.fpay_num, b.fpay_num)) fpay_num,
            sum(coalesce(a.fparty_num,0) + b.fparty_tnum) fparty_tnum,
            sum(coalesce(a.fpay_num,0) + b.fpay_tnum) fpay_tnum,

            substr(coalesce(a.fsignup_at, b.fsignup_at),1,10) dt

        from stage.user_push_orc a

        join dim.push_user b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid

       where a.dt='%(statdate)s'

        group by coalesce(a.fuid,b.fuid), coalesce(a.fbpid, b.fbpid),substr(coalesce(a.fsignup_at, b.fsignup_at),1,10)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert into table dim.push_user
        partition(dt)
        select
            a.fbpid,
            a.fuid,
            max(c.fgamename) fgamename,
            max(c.fplatformname) fplatformname,
            max(a.fappid) fappid,
            max(a.ftoken) ftoken,
            max(a.flts_at) flts_at,
            max(a.flogin_at) flogin_at,
            max(a.fsignup_at) fsignup_at,
            max(a.fis_open) fis_open,
            max(a.fversion_info) fversion_info,
            max(a.fuser_gamecoins) fuser_gamecoins,
            max(a.fm_dtype) fm_dtype,
            max(a.fentrance_id) fentrance_id,
            max(a.fis_paid) fis_paid,
            max(a.fvip_level) fvip_level,
            max(a.fgender) fgender,
            max(a.fchannel_code) fchannel_code,
            max(a.fparty_num) fparty_num,
            max(a.fpay_num) fpay_num,
            sum(a.fparty_num) fparty_tnum,
            sum(a.fpay_num) fpay_tnum

            substr(coalesce(a.fsignup_at, b.fsignup_at),1,10) dt

        from stage.user_push_orc a

        left join dim.push_user b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid

        join dim.bpid_map c
          on a.fbpid = c.fbpid

       where a.dt='%(statdate)s' and b.fbpid is null

        group by coalesce(a.fuid,b.fuid), coalesce(a.fbpid, b.fbpid),substr(coalesce(a.fsignup_at, b.fsignup_at),1,10)

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


#生成统计实例
a = load_push_user(sys.argv[1:])
a()
