#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_stream_all(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.payment_stream_all
        (
        fdate               date,
        fuid                string,
        fbpid               string,
        ffirsttime          string,
        flasttime           string,
        fusd                decimal(20,7),
        fnum                bigint,
        fm_imei             string,
        fplatform_uid       string,
        sid                 bigint,
        appid               bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 0


        # 付费用户全量表
        # 更新表8.2日开始数据有效，所以之前的不能用这个逻辑来更新，8.1日已经生成了不用再更新
        # if '%(ld_begin)s' < to_date('20140817','yyyymmdd') then
        #     return;
        # end if;

        hql = """
        insert overwrite table stage.payment_stream_all partition
        (dt = "%(ld_begin)s")
        select '%(ld_begin)s' as fdate,
            fuid, fbpid,
            min(ffirsttime) as ffirsttime,
            max(flasttime) as flasttime,
            sum(fusd) as fusd,
            sum(fnum) as fnum,
            max(fm_imei) as fm_imei,
            nvl(fplatform_uid,'') fplatform_uid, sid, appid
        from
        (
            -- 历史数据
            select fuid, fbpid, ffirsttime, flasttime, fusd, fnum, fm_imei, fplatform_uid, sid, appid
            from stage.payment_stream_all
            where dt = date_add('%(ld_begin)s', -1)
            and fuid is not null and fbpid is not null -- 正常的uid和bpid
            union all
            -- 当天的数据
            select fuid, fbpid,
                fdate as ffirsttime,
                case when pstatus = 2 then fsucc_time else null end as flasttime,
                case when pstatus = 2 then fusd when pstatus = 3 then -fusd else 0 end as fusd,
                case when pstatus = 2 then 1 when pstatus = 3 then -1 else 0 end as fnum,
                fm_imei,
                case when fuid is null then fplatform_uid else null end as fplatform_uid,
                case when fbpid is null then sid else null end as sid,
                case when fbpid is null then appid else null end as appid
            from stage.payment_stream_mid
           where dt = '%(ld_begin)s'
             and (pstatus = 2 or pstatus = 3 and fdate < '%(ld_begin)s')
        ) a
        group by fuid, fbpid, nvl(fplatform_uid,''), sid, appid
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
    a = agg_payment_stream_all(stat_date)
    a()
