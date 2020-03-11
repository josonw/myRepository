#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class update_user_info(BaseStat):
    """建立游戏维度表
    """
    def stat(self):
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""set  hive.optimize.sort.dynamic.partition=true""")
        if res != 0:
            return res
        # self.hq.debug = 0

        hql = """
        insert overwrite table stage.user_dim partition(dt)
        select -- a.fsk,
            a.fbpid,
            a.fuid,
            a.fplatform_uid,
            a.fsignup_at,
            a.fname,
            a.fdisplay_name,
            a.fgender,
            a.fappid,
            a.fgame_coin,
            a.fby_coin,
            a.fpoint,
            a.fip,
            a.flanguage,
            a.fcountry,
            a.fcity,
            a.fage,
            a.fmail,
            a.ftimezone,
            a.fbirthday,
            a.fbloodtype,
            a.freligion,
            a.fhometown,
            a.fmarriage,
            a.fweight,
            a.fheight,
            a.fprofession,
            a.fschool_record,
            a.fmotto,
            a.fwishlist,
            a.fintro,
            a.finviter_uid,
            a.finviter_code,
            a.freference_type,
            a.freference,
            a.fhttp_version,
            a.fuser_agent,
            a.fgame_svn_info,
            a.fgame_auth_host,
            a.fuuid,
            -- 将异常的等级信息初始化为0
            if( a.fgrade is null, b.fgrade, a.fgrade ) fgrade,
            a.ffriends_num,
            a.fappfriends_num,
            a.fentrance_id,
            a.fversion_info,
            a.flastlogin_at,
            a.flastpay_at,
            a.flastsync_at,
            a.fad_code,
            a.fsource_path,
            a.fm_dtype,
            a.fm_pixel,
            a.fm_os,
            a.fm_network,
            a.fm_operator,
            a.fm_imei,
            a.dt
        from tmp.user_dim_0423 b
        right outer join stage.user_dim a
        on a.fbpid = b.fbpid and a.fuid = b.fuid;
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
a = update_user_info(statDate)
a()
