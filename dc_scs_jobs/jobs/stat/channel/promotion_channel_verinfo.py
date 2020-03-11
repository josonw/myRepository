#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class promotion_channel_verinfo(BaseStat):

    def create_tab(self):
        hql = """
        create external table if not exists analysis.marketing_channel_pkg_info
        (
          fid          bigint,
          fname        string,
          ftrader_id   bigint,
          fgame_id     bigint,
          fplatform_id bigint,
          fcomment     string,
          fstate       bigint,
          fadd_time    string,
          fappkey      string,
          fpublish_at  string,
          fdims        string,
          is_recovery  bigint,
          fcoop_type   string
        );

        create external table if not exists analysis.marketing_channel_trader_info
        (
          fid              bigint,
          fname            string,
          faccount         string,
          fpassword        string,
          fcomment         string,
          fcurrency        bigint,
          fstate           bigint,
          fjoin_time       string,
          flast_login_time string,
          is_recovery      bigint,
          fbank_info       string,
          fbelong_group    string
        );

        create table if not exists analysis.channel_verinfo_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk    bigint,
          fversionfsk     bigint,
          fterminalfsk    bigint,
          fpkg_channel_id varchar(64),
          fchannel_id     varchar(64),
          fcli_verinfo    varchar(100),
          freg_num        bigint,
          factive_num     bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []
        hql = """
        use analysis;
        create view if not exists dc_channel_package as
        select fid fpkg_id, ftrader_id fpkg_channel_id, fname fpkg_desc
        from analysis.marketing_channel_pkg_info;

        use analysis;
        create view if not exists dc_channel as
        select fid fchannel_id, fname name from analysis.marketing_channel_trader_info;


        insert overwrite table analysis.channel_verinfo_fct partition
          (dt = "%(ld_begin)s")
          select fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fpkg_channel_id,
                 fchannel_id,
                 fcli_verinfo,
                 max(freg_num) freg_num,
                 max(factive_num) factive_num
            from (SELECT '%(ld_begin)s' fdate,
                         fgamefsk,
                         fplatformfsk,
                         fversion_old fversionfsk,
                         d.fterminalfsk,
                         fpkg_channel_id,
                         fchannel_id,
                         fcli_verinfo,
                         count(distinct a.fuid) freg_num,
                         0 factive_num
                    from stage.channel_market_reg_mid a
                    join analysis.dc_channel_package b
                      on a.fchannel_id = b.fpkg_id
                    join dim.bpid_map d
                      on a.fbpid = d.fbpid
                   where a.dt = '%(ld_begin)s'
                   group by fgamefsk,
                            fplatformfsk,
                            fversion_old,
                            d.fterminalfsk,
                            fpkg_channel_id,
                            a.fchannel_id,
                            fcli_verinfo
                  union all
                  SELECT '%(ld_begin)s' fdate,
                         fgamefsk,
                         fplatformfsk,
                         fversion_old fversionfsk,
                         d.fterminalfsk,
                         fpkg_channel_id,
                         fchannel_id,
                         fcli_verinfo,
                         0 freg_num,
                         count(distinct a.fuid) factive_num
                    from stage.channel_market_active_mid a
                    join analysis.dc_channel_package b
                      on a.fchannel_id = b.fpkg_id
                    join dim.bpid_map d
                      on a.fbpid = d.fbpid
                   where a.dt = '%(ld_begin)s'
                   group by fgamefsk,
                            fplatformfsk,
                            fversion_old,
                            d.fterminalfsk,
                            fpkg_channel_id,
                            a.fchannel_id,
                            fcli_verinfo) as abc
           group by fdate, fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,
                    fpkg_channel_id,
                    fchannel_id,
                    fcli_verinfo
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
    a = promotion_channel_verinfo(stat_date)
    a()