#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_stream_mid(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.payment_stream_mid
        (
          flts_at          string,
          fbpid            varchar(50),
          fdate            string,
          fplatform_uid    varchar(256),
          fis_platform_uid decimal(1),
          forder_id        varchar(256),
          fcoins_num       decimal(20,7),
          frate            decimal(20,7),
          fusd             decimal(20,7),
          fm_id            varchar(256),
          fm_name          varchar(256),
          fp_id            decimal(20),
          fp_name          varchar(256),
          fchannel_id      varchar(256),
          fimei            varchar(256),
          fsucc_time       string,
          fcallback_time   date,
          fp_type          decimal(10),
          fp_num           decimal(30),
          fuid             decimal(20),
          fm_imei          varchar(128),
          fis_first        decimal(5),
          fversion_info    varchar(64),
          fchannel_code    varchar(64),
          fad_code         varchar(64),
          mid              decimal(20),
          buyer            varchar(256),
          sid              decimal(11),
          appid            decimal(11),
          appname          varchar(128),
          childid          varchar(128),
          is_lianyun       decimal(5),
          pmode            decimal(5),
          pmodename        varchar(128),
          companyid        decimal(10),
          tag              decimal(5),
          statid           decimal(10),
          statname         varchar(128),
          pcoins           decimal(20),
          pchips           decimal(20),
          pcard            decimal(11),
          pnum             decimal(11),
          pcoinsnow        decimal(20),
          pdealno          varchar(256),
          pbankno          varchar(256),
          fdesc            varchar(256),
          pstatus          decimal(3),
          pamount_rate     decimal(20,2),
          pamount_unit     varchar(256),
          pamount_usd      decimal(20,2),
          ext_2            decimal(11),
          ext_3            decimal(11),
          ext_4            varchar(64),
          ext_5            varchar(255),
          ext_8            varchar(255),
          ext_9            varchar(255),
          ext_10           varchar(255),
          m_at             varchar(64)
        )
        partitioned by (dt date)        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 0

        hql = """
        insert overwrite table stage.payment_stream_mid partition(dt)
        select flts_at,fbpid,fdate,fplatform_uid,fis_platform_uid,
            forder_id,fcoins_num,frate,fusd,fm_id,fm_name,fp_id,fp_name,fchannel_id,fimei,
            fsucc_time,fcallback_time,fp_type,fp_num,
            fuid,fm_imei,fis_first,fversion_info,fchannel_code,fad_code,
            mid,buyer,sid,appid,appname,childid,is_lianyun,
            pmode,pmodename,companyid,tag,statid,statname,
            pcoins,pchips,pcard,pnum,pcoinsnow,pdealno,pbankno,fdesc,pstatus,
            pamount_rate,pamount_unit,pamount_usd,
            ext_2,ext_3,ext_4,ext_5,ext_8,ext_9,ext_10,m_at,
            dt
        from
        (
            select dt,flts_at,fbpid,fdate,fplatform_uid,fis_platform_uid,
                forder_id,fcoins_num,frate,fusd,fm_id,fm_name,fp_id,fp_name,fchannel_id,fimei,
                fsucc_time,fcallback_time,fp_type,fp_num,
                fuid,fm_imei,fis_first,fversion_info,fchannel_code,fad_code,
                mid,buyer,sid,appid,appname,childid,is_lianyun,
                pmode,pmodename,companyid,tag,statid,statname,
                pcoins,pchips,pcard,pnum,pcoinsnow,pdealno,pbankno,fdesc,pstatus,
                pamount_rate,pamount_unit,pamount_usd,
                ext_2,ext_3,ext_4,ext_5,ext_8,ext_9,ext_10,m_at,
                row_number() over(partition by forder_id order by nvl(ext_2,0) desc) as flag
            from stage.payment_stream_mid_pre
            where dt >= date_add('%(ld_begin)s', -150) and dt < '%(ld_end)s'
        ) as abc
        where flag = 1

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
    a = agg_payment_stream_mid(stat_date)
    a()
