#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_register_day(BaseStat):
    """用户注册表
    """
    def create_tab(self):
        #后面新加字段：fmonthpaycnt bigint,fmonthpayusercnt bigint,fmonthincome decimal(38,2)
        hql = """create table if not exists analysis.user_register_fct
                (
                fdate date,
                fplatformfsk bigint,
                fgamefsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fagefsk bigint,
                foccupationalfsk string,
                fsexfsk bigint,
                fcityfsk string,
                fregcnt bigint,
                fentrancefsk bigint,
                fversioninfofsk string,
                fsignupcnt bigint,
                fsigndpucnt bigint,
                fdevregcnt bigint
                )
                partitioned by (dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """drop table if exists stage.user_register_fct_tmp;
        create table stage.user_register_fct_tmp
                (
                fdate date,
                fplatformfsk bigint,
                fgamefsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fagefsk bigint,
                foccupationalfsk string,
                fsexfsk bigint,
                fcityfsk string,
                fregcnt bigint,
                fentrancefsk bigint,
                fversioninfofsk string,
                fsignupcnt bigint,
                fsigndpucnt bigint,
                fdevregcnt bigint,
                fmonthpaycnt bigint,
                fmonthpayusercnt bigint,
                fmonthincome decimal(38,2)
                )
        stored as orc;
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        query = { 'statdate':self.stat_date,'ld_monthbegin':PublicFunc.trunc(statDate, 'MM'),'ld_1daylater': PublicFunc.add_days(statDate, 1)}
        res = self.hq.exe_sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        # self.hq.debug = 0
        hql = """
        insert into table  stage.user_register_fct_tmp
          select '%(statdate)s' fdate,
                 bpm.fplatformfsk,
                 bpm.fgamefsk,
                 bpm.fversionfsk,
                 bpm.fterminalfsk,
                 fage,
                 fprofession,
                 fgender,
                 fcity,
                 count(distinct fuid) fregcnt,
                 fentrance_id fentrancefsk,
                 fversion_info fversioninfofsk,
                 count(distinct fuid) fsignupcnt,
                 sum(COALESCE(fpaycnt,0)) fsigndpucnt,
                 count(distinct fm_imei) fdevregcnt,
                 0 fmonthpaycnt,
                 0 fmonthpayusercnt,
                 0 fmonthincome
            from stage.user_dim ud
            left join (
                select fbpid, fplatform_uid, count(1) fpaycnt from
                stage.payment_stream_stg where dt='%(statdate)s'
                group by fbpid, fplatform_uid
               ) b
            on ud.fbpid=b.fbpid
            and ud.fplatform_uid=b.fplatform_uid
            join analysis.bpid_platform_game_ver_map bpm
              on ud.fbpid = bpm.fbpid
           where ud.dt = '%(statdate)s'
           group by bpm.fplatformfsk,
                    bpm.fgamefsk,
                    bpm.fversionfsk,
                    bpm.fterminalfsk,
                    fentrance_id,
                    fversion_info,
                    fage,
                     fprofession,
                     fgender,
                     fcity
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert into table stage.user_register_fct_tmp
          select '%(statdate)s' fdate,
                 bpm.fplatformfsk,
                 bpm.fgamefsk,
                 bpm.fversionfsk,
                 bpm.fterminalfsk,
                 fage,
                 fprofession,
                 fgender,
                 fcity,
                 0 fregcnt,
                 fentrance_id fentrancefsk,
                 fversion_info fversioninfofsk,
                 0 fsignupcnt,
                 0 fsigndpucnt,
                 0 fdevregcnt,
                 nvl(count(b.fplatform_uid),0) fmonthpaycnt,
                 nvl(count(distinct b.fplatform_uid),0) fmonthpayusercnt,
                 nvl(sum(b.fcoins_num *b.frate),0) fmonthincome
            from stage.user_dim ud
            left join (
                select fbpid, fplatform_uid, fcoins_num,frate from
                stage.payment_stream_stg where dt>='%(ld_monthbegin)s'
                and dt < '%(ld_1daylater)s'
               ) b
            on ud.fbpid=b.fbpid
            and ud.fplatform_uid=b.fplatform_uid
            join analysis.bpid_platform_game_ver_map bpm
              on ud.fbpid = bpm.fbpid
           where ud.dt >= '%(ld_monthbegin)s' and ud.dt < '%(ld_1daylater)s'
           group by bpm.fplatformfsk,
                    bpm.fgamefsk,
                    bpm.fversionfsk,
                    bpm.fterminalfsk,
                    fentrance_id,
                    fversion_info,
                    fage,
                    fprofession,
                    fgender,
                    fcity
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.user_register_fct
        partition(dt = '%(statdate)s' )
          select fdate,
                 fplatformfsk,
                 fgamefsk,
                 fversionfsk,
                 fterminalfsk,
                 fagefsk,
                 foccupationalfsk,
                 fsexfsk,
                 fcityfsk,
                 max(fregcnt) fregcnt,
                 fentrancefsk,
                 fversioninfofsk,
                 max(fsignupcnt) fsignupcnt,
                 max(fsigndpucnt) fsigndpucnt,
                 max(fdevregcnt) fdevregcnt,
                 max(fmonthpaycnt) fmonthpaycnt,
                 max(fmonthpayusercnt) fmonthpayusercnt,
                 max(fmonthincome) fmonthincome
            from stage.user_register_fct_tmp
            group by fdate,
                 fplatformfsk,
                 fgamefsk,
                 fversionfsk,
                 fterminalfsk,
                 fagefsk,
                 foccupationalfsk,
                 fsexfsk,
                 fcityfsk,
                 fentrancefsk,
                 fversioninfofsk""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_user_register_day(statDate)
a()
