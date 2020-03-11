#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_stream_mid_pre(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.payment_stream_mid_pre
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
          fcallback_time   string,
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
         insert overwrite table stage.payment_stream_mid_pre partition(dt="%(stat_date)s")
        select flts_at, fbpid, pstarttime as fdate,
            sitemid as fplatform_uid, 1 as fis_platform_uid,
            pid as forder_id,
            pamount as fcoins_num, frate, fusd,
            statid as fm_id, statname as fm_name,
            payconfid as fp_id, null as fp_name,
            ext_6 as fchannel_id, ext_7 as fimei,
            pendtime as fsucc_time, from_unixtime(ext_1,'yyyy-MM-dd HH:mm:ss') as fcallback_time,
            case when pcoins <> 0 then 1
                 when pchips <> 0 then 2
                 when pcard  <> 0 then 3
            else 0 end as fp_type,
            case when pcoins <> 0 then pcoins
                 when pchips <> 0 then pchips
                 when pcard  <> 0 then pnum
            else 0 end as fp_num,

            -- 8,17日修改，mid大于0时记录的就是uid
            case when mid > 0 then mid else ty.fuid end as fuid,
            ty.fm_imei, ty.fis_first, ty.fversion_info, ty.fchannel_code, ty.fad_code,

            mid, buyer, sid,
            appid, appname, childid, is_lianyun,
            pmode, pmodename, companyid, tag, statid, statname,
            pcoins, pchips, pcard, pnum,
            pcoinsnow, pdealno, pbankno, fdesc, pstatus, pamount_rate, pamount_unit, pamount_usd,
            ext_2, ext_3, ext_4, ext_5, ext_8, ext_9, ext_10, m_at
        from
        (
            select ta.pmodename, ta.companyid, ta.tag, ta.statid, ta.statname,
                tb.*
            from
            (
                -- 关联支付渠道配置表，获取渠道等信息
                select sid, pmode,
                    max(pmodename) as pmodename, max(companyid) as companyid, max(tag) as tag,
                    max(statid) as statid, max(statname) as statname
                from analysis.paycenter_chanel_dim
                group by sid, pmode
            )ta
            right join
            (
                -- 关联支付bpid配置表，获取bpid等信息
                select tm.bpid as fbpid, tm.appname, tm.childid, tm.is_lianyun,
                    tn.*
                from
                (
                    select sid, appid,
                        max(bpid) as bpid, max(appname) as appname,
                        max(childid) as childid, 0 as is_lianyun
                    from analysis.paycenter_apps_dim
                    group by sid, appid
                )tm
                right join
                (
                    -- 关联汇率配置表，获取汇率信息
                    select t1.rate as frate, t2.pamount*t1.rate as fusd,
                        t2.*
                    from analysis.pay_rate_dim t1
                    right join
                    (
                        -- 每条订单取最后一条
                        select *
                        from
                        (
                            select flts_at, pid, mid, sitemid, buyer, sid, appid, pmode, pamount, pcoins, pchips, pcard, pnum,
                                payconfid, pcoinsnow, pdealno, pbankno, fdesc, pstarttime, pendtime, pstatus, pamount_rate, pamount_unit, pamount_usd,
                                ext_1, ext_6, ext_7,
                                ext_2, ext_3, ext_4, ext_5, ext_8, ext_9, ext_10, m_at,
                                -- 这个处理是为了和配置表做关联
                                case when pamount_unit is not null then -1 else sid end as sid_tmp,
                                case when pamount_unit is not null then -1 else pmode end as pmode_tmp,
                                case when pamount_unit is not null then pamount_unit else '-1' end as  unit_tmp,
                                row_number() over(partition by pid order by nvl(ext_2,0) desc) as flag
                            from stage.update_order_info_stg
                            where dt = '%(ld_begin)s'
                        ) as temp_a
                        where flag = 1
                        --and pstatus <> 0 -- pstatus没什么变化的记录不取
                    ) t2
                    on t1.sid = t2.sid_tmp and t1.pmode = t2.pmode_tmp and t1.unit = t2.unit_tmp
                ) tn
                on tm.sid = tn.sid and tm.appid = tn.appid
            ) tb
            on ta.sid = tb.sid and ta.pmode = tb.pmode
        ) tx
        left join
        (
            -- 每天约有2%%的用户无法获取uid，所以关联一下前一天和当天的登陆和注册用户
            select fplatform_uid, fbpid as bpid,
                max(fuid) as fuid, max(fm_imei) as fm_imei, max(fis_first) as fis_first,
                max(fversion_info) as fversion_info, max(fchannel_code) as fchannel_code, max(fad_code) as fad_code
            from
            (
                select fplatform_uid, fbpid, fuid, fm_imei, fis_first, fversion_info, fchannel_code, fad_code
                from stage.user_login_stg
                where dt >= '%(ld_begin_pre)s' and dt < '%(ld_end)s'
                    and fbpid is not null
                union all
                select fplatform_uid, fbpid, fuid, fm_imei, null as fis_first, fversion_info, fchannel_code, fad_code
                from stage.user_signup_stg
                where dt >= '%(ld_begin_pre)s' and dt < '%(ld_end)s'
                    and fbpid is not null
            ) as temp_b
            group by fplatform_uid, fbpid
        ) ty
        on tx.sitemid = ty.fplatform_uid and tx.fbpid = ty.bpid

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
    a = agg_payment_stream_mid_pre(stat_date)
    a()
