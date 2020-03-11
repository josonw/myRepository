#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_pay_channel_province_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_channel_province_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fm_id                       string,
            fcountry                    string,
            fprovince                   string,
            fpayuser_cnt                bigint,
            fpayuser_num                bigint,
            fmoney                      decimal(20,2),
            forder                      bigint
        )
        partitioned by (dt date)
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        alter table pay_channel_province_fct drop partition(dt = "%(stat_date)s")
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """
            drop table if exists analysis.pay_channel_province_fct_temp_%(num_begin)s;
            create table analysis.pay_channel_province_fct_temp_%(num_begin)s as
                select a.dt,a.fbpid, max(case when a.fuid>0 then a.fuid else b.fuid end) fuid,
                        a.forder_id,  a.fplatform_uid, a.fcoins_num, a.frate ,a.fm_id, a.pstatus
                    FROM stage.payment_stream_all_stg a
                    LEFT JOIN stage.user_dim b
                    on a.fbpid = b.fbpid
                    and a.fplatform_uid = b.fplatform_uid
                  WHERE a.dt = "%(stat_date)s"
                group by a.dt,a.fbpid, a.forder_id, a.fplatform_uid, a.fcoins_num, a.frate ,a.fm_id, a.pstatus
        ;
        drop table if exists analysis.user_location_temp_%(num_begin)s;
        create table analysis.user_location_temp_%(num_begin)s
          as
        select b.key.bpid fbpid,
                cast(b.key.uid as bigint) fuid,
                b.country,b.province
                from hbase.user_location_all b
        group by b.key.bpid,b.key.uid,b.country,b.province
            """ % self.hql_dict
        hql_list.append( hql )

        hql = """
          insert into table analysis.pay_channel_province_fct partition(dt = "%(stat_date)s")
            SELECT /*+ MAPJOIN(a) */ '%(stat_date)s' fdate,
                                   m.fgamefsk,
                                   m.fplatformfsk,
                                   m.fversionfsk,
                                   m.fterminalfsk,
                                   a.fm_id,
                                   coalesce(b.country,'未知') country,
                                   coalesce(b.province,'未知') province,
                                   count(DISTINCT case when a.pstatus=2 then a.forder_id else null end) fpayuser_cnt,
                                   count(DISTINCT case when a.pstatus=2 then a.fplatform_uid else null end) fpayuser_num,
                                   sum(case when a.pstatus=2 then a.fcoins_num * a.frate else 0 end) fmoney,
                                   count(*) fordercnt
            from
            analysis.pay_channel_province_fct_temp_%(num_begin)s a
            LEFT JOIN analysis.user_location_temp_%(num_begin)s  b ON b.fbpid=a.fbpid
            AND b.fuid=a.fuid
            JOIN analysis.bpid_platform_game_ver_map m ON a.fbpid = m.fbpid
            GROUP BY m.fgamefsk,
                     m.fplatformfsk,
                     m.fversionfsk,
                     m.fterminalfsk,
                     a.fm_id,
                     coalesce(b.country,'未知') ,
                     coalesce(b.province,'未知')

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.pay_channel_province_fct partition(dt = "%(stat_date)s")
        select
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fm_id,
            a.fcountry  fcountry,
            case when (a.fcountry='中国' and b.fprovince is null) then '未知' else a.fprovince end fprovince,
            sum(fpayuser_cnt) fpayuser_cnt,
            sum(fpayuser_num) fpayuser_num,
            sum(fmoney)       fmoney      ,
            sum(forder)       forder
        from analysis.pay_channel_province_fct a
        left join analysis.geography_info_dim b
        on a.fcountry = b.fcountry
        and a.fprovince = b.fprovince
        where dt = "%(stat_date)s"
        group by
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fm_id,
            a.fcountry,
            case when (a.fcountry='中国' and b.fprovince is null) then '未知' else a.fprovince end;
        drop table if exists analysis.pay_channel_province_fct_temp_%(num_begin)s;
        drop table if exists analysis.user_location_temp_%(num_begin)s;

        """ % self.hql_dict
        hql_list.append( hql )


        result = self.exe_hql_list(hql_list)
        return result



if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_pay_channel_province_data(stat_date)
    a()
