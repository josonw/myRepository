#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_user_dim(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """
        create external table if not exists stage.user_dim_check
        (
            fsk           bigint,
            fbpid         varchar(50),
            fuid          bigint,
            fplatform_uid varchar(50),
            fsignup_at    string,
            fname         varchar(200),
            fdisplay_name varchar(400),
            fgender       bigint,
            fappid        bigint,
            fgame_coin    decimal(20,2),
            fby_coin      decimal(20,2),
            fpoint        decimal(20,2),
            fip           varchar(20),
            flanguage     varchar(50),
            fcountry      varchar(50),
            fcity         varchar(50),
            fage          bigint,
            fmail         varchar(100),
            ftimezone     varchar(50),
            fbirthday     string,
            fbloodtype    varchar(10),
            freligion     varchar(20),
            fhometown     varchar(100),
            fmarriage     decimal(10,2),
            fweight       decimal(10,2),
            fheight       decimal(10,2),
            fprofession     varchar(50),
            fschool_record  varchar(50),
            fmotto          varchar(200),
            fwishlist       varchar(200),
            fintro          varchar(200),
            finviter_uid    bigint,
            finviter_code   varchar(50),
            freference_type bigint,
            freference      varchar(100),
            fhttp_version   varchar(100),
            fuser_agent     varchar(200),
            fgame_svn_info  varchar(100),
            fgame_auth_host varchar(100),
            fuuid           bigint,
            fgrade          bigint, -- 注册表没有的字段
            ffriends_num    bigint,
            fappfriends_num bigint,
            fentrance_id    bigint,
            fversion_info   varchar(50),
            flastlogin_at   string, -- 注册表没有的字段
            flastpay_at     string, -- 注册表没有的字段
            flastsync_at    string, -- 注册表没有的字段
            fad_code        varchar(50),
            fsource_path    varchar(100),
            fm_dtype        varchar(100),
            fm_pixel        varchar(100),
            fm_os           varchar(100),
            fm_network      varchar(100),
            fm_operator     varchar(100),
            fm_imei         varchar(100)
        )
        partitioned by (dt date)
        stored as orc
        location '/dw/stage/user_dim'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        res = self.hq.exe_sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0:
            return res

        # self.hq.debug = 0
        hql = """
        insert overwrite table stage.user_dim_check partition ( dt='%(statdate)s' )
        select null fsk,
            a.fbpid,
            a.fuid,
            a.fplatform_uid,
            a.fsignup_at,
            null fname,
            null fdisplay_name,
            a.fgender,
            null fappid,
            null fgame_coin,
            null fby_coin,
            null fpoint,
            a.fip,
            a.flanguage,
            a.fcountry,
            a.fcity,
            a.fage,
            null fmail,
            null ftimezone,
            null fbirthday,
            null fbloodtype,
            null freligion,
            null fhometown,
            null fmarriage,
            null fweight,
            null fheight,
            a.fprofession,
            null fschool_record,
            null fmotto,
            null fwishlist,
            null fintro,
            null finviter_uid,
            null finviter_code,
            null freference_type,
            null freference,
            null fhttp_version,
            null fuser_agent,
            null fgame_svn_info,
            null fgame_auth_host,
            null fuuid,
            0 fgrade, -- 注册表无该信息，需要别的表来刷新
            a.ffriends_num,
            a.fappfriends_num,
            a.fentrance_id,
            a.fversion_info,
            null flastlogin_at,  -- 注册表无该信息，需要别的表来刷新
            null flastpay_at,    -- 注册表无该信息，需要别的表来刷新
            null flastsync_at,   -- 注册表无该信息，需要别的表来刷新
            a.fad_code,
            a.fsource_path,
            a.fm_dtype,
            a.fm_pixel,
            a.fm_os,
            a.fm_network,
            a.fm_operator,
            a.fm_imei
        from
        (
            select fbpid, fuid,
                max(fplatform_uid) fplatform_uid,
                min(fsignup_at) fsignup_at,
                max(fip) fip,
                max(fgender) fgender,
                max(fage) fage,
                max(flanguage) flanguage,
                max(fcountry) fcountry,
                max(fcity) fcity,
                max(ffriends_num) ffriends_num,
                max(fappfriends_num) fappfriends_num,
                max(fprofession) fprofession,
                max(fentrance_id) fentrance_id,
                max(fversion_info) fversion_info,
                max(fchannel_code) fchannel_code,
                max(fad_code) fad_code,
                max(fm_dtype) fm_dtype,
                max(fm_pixel) fm_pixel,
                max(fm_imei) fm_imei,
                max(fm_os) fm_os,
                max(fm_network) fm_network,
                max(fm_operator) fm_operator,
                max(fmnick) fmnick,
                max(fmname) fmname,
                max(femail) femail,
                max(fmobilesms) fmobilesms,
                max(fsource_path) fsource_path
            from user_signup_stg
            where dt = '%(statdate)s'
            group by fbpid, fuid
        ) a
        left outer join
        (
            select fbpid, fuid
            from stage.user_dim
            where dt < '%(statdate)s'
        ) b
        on a.fuid = b.fuid and a.fbpid = b.fbpid
        where b.fbpid is null
        """ % { 'statdate':self.stat_date }

        # res = 0
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
a = load_user_dim(statDate)
a()
