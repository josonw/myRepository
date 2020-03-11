#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class load_user_info_all_hbase(BaseStat):

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        """将每天数据插到HBase的映射表"""
        # 注册信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, fsignup_at,
                fchannel_code,
                fm_dtype,
                fm_pixel,
                fm_imei,
                fip,
                fip_country,
                fip_province,
                fip_city)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 max(fsignup_at)      fsignup_at,
                 max(fchannel_code)   fchannel_code,
                 max(fm_dtype)        fm_dtype,
                 max(fm_pixel)        fm_pixel,
                 max(fm_imei)         fm_imei,
                 max(fip)             fip,
                 max(fip_country)     fip_country,
                 max(fip_province)    fip_province,
                 max(fip_city)        fip_city
            from stage.user_signup_stg
           where dt = '%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
           group by fbpid, fuid
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 登录信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, ffirst_log_date, flast_log_date, flogin_num, flogin_day)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 ffirst_date ffirst_log_date,
                 flast_date flast_log_date,
                 flogin_num,
                 flogin_day
            from stage.user_login_all
           where dt='%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and flast_date >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res


        # 在线时长信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, fonline_time)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 fonline_time
            from stage.user_online_time_all
           where dt='%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and flogin_first_time >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 活跃信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, ffirst_act_date, flast_act_date, fact_day)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 ffirst_act_date,
                 flast_act_date,
                 fact_day
            from stage.user_active_all
           where dt='%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
           and ffirst_act_date is not NULL
           and flast_act_date >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 破产信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, ffirst_rupt_date, flast_rupt_date, fbankrupt_cnt, fbankrupt_day)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 ffirst_rupt_date,
                 flast_rupt_date,
                 fbankrupt_cnt,
                 fbankrupt_day
            from stage.user_bankrupt_all
           where dt='%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
            and flast_rupt_date >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 破产救济信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, ffirst_relieve_time,
                 flast_relieve_time,
                 frelieve_cnt,
                 frelieve_day,
                 frelieve_gamecoins)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 ffirst_relieve_time,
                 flast_relieve_time,
                 frelieve_cnt,
                 frelieve_day,
                 fgamecoins            frelieve_gamecoins
            from stage.user_bankrupt_relieve_all
           where dt='%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
            and flast_relieve_time >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 牌局信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, ffirst_play_date,
                 flast_play_date,
                 fcharge,
                 fplay_inning,
                 fplay_time,
                 fplay_day,
                 fwin_inning,
                 flose_inning,
                 fwin_gamecoins,
                 flose_gamecoins
          )
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 ffirst_play_date,
                 flast_play_date,
                 fcharge,
                 fplay_inning,
                 fplay_time,
                 fplay_day,
                 fwin_inning,
                 flose_inning,
                 fwin_gamecoins,
                 flose_gamecoins
              from stage.user_gameparty_all
             where dt='%(ld_daybegin)s'
               and fbpid is not NULL
               and fuid is not NULL
               and fbpid != '0'
               and fuid != 0
               and flast_play_date >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 场次信息，在大厅模式下就是子游戏信息
        hql = """
            drop table if exists stage.user_gameparty_pname_all_tmp;
            create table stage.user_gameparty_pname_all_tmp as
            select key, fpname fmax_pname
            from(
            select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                         fpname,
                         row_number() over(partition by fbpid, fuid order by fparty_num desc) fnrow
                    from stage.user_gameparty_pname_all
                   where dt='%(ld_daybegin)s'
                     and fbpid is not NULL
                     and fuid is not NULL
                     and fbpid != '0'
                     and fuid != 0) t
           where fnrow = 1
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        hql = """
          insert into stage.user_info_all_hbase
          (key, fmax_pname)
          select  key, fmax_pname
            from stage.user_gameparty_pname_all_tmp
           where fmax_pname is not null
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        hql = """
          drop table if exists stage.user_gameparty_pname_all_tmp;
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 金币信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, ffirst_act_gc_date,
                flast_act_gc_date,
                fgamecoins_num
          )
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 ffirst_act_date   ffirst_act_gc_date,
                 flast_act_date    flast_act_gc_date,
                 fgamecoins_num
            from stage.user_gamecoins_all
           where dt='%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and flast_act_date >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        # 付费信息
        hql = """
          insert into stage.user_info_all_hbase
          (key, ffirst_pay_time,
                flast_pay_time,
                fusd,
                fpay_num
          )
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 ffirsttime ffirst_pay_time,
                 flasttime flast_pay_time,
                 fusd,
                 fnum fpay_num
            from stage.payment_stream_all
           where dt='%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and flasttime >= '%(ld_daybegin)s'
        """
        res = self.hq.exe_sql(hql % query)
        if res != 0: return res

        """
        将同步数据插到HBase的映射表
        因为如果字段有NULL会插入失败，所以需要分开处理NULL字段
        """

        hql = """
          insert into stage.user_info_all_hbase
           (key, fgamecoins_num)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 max(fgamecoin_num)         fgamecoins_num
            from user_async_stg
           where dt = '%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and fgamecoin_num is not NULL
           group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
          insert into stage.user_info_all_hbase
          (key, fbycoins_num)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 max(fbycoin_num)           fbycoins_num
            from user_async_stg
           where dt = '%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and fbycoin_num is not NULL
           group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
          insert into stage.user_info_all_hbase
          (key,  flast_login_ip)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 max(fip)                   flast_login_ip
            from user_async_stg
           where dt = '%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and fip is not NULL
           group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
          insert into stage.user_info_all_hbase
           (key, fphone)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 max(fphone)                fphone
            from user_async_stg
           where dt = '%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and fphone is not NULL
           group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
          insert into stage.user_info_all_hbase
          (key, flevel)
          select named_struct('fbpid',cast(fbpid as string), 'fuid', cast(fuid as string)) key,
                 max(flevel)                flevel
            from user_async_stg
           where dt = '%(ld_daybegin)s'
             and fbpid is not NULL
             and fuid is not NULL
             and fbpid != '0'
             and fuid != 0
             and flevel is not NULL
           group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = load_user_info_all_hbase(statDate)
    a()
