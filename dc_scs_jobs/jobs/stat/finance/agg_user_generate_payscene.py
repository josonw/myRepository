#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_generate_payscene(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.scene_dim
        (
            fsk                     bigint,
            fgamefsk                bigint,
            fname                   varchar(100),
            fdisname                varchar(100)
        );

        create table if not exists analysis.user_generate_order_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fpname                      varchar(100),
            fsubname                    varchar(100),
            fanto                       varchar(100),
            fpay_scene                  varchar(100),
            fis_bankrupt                int,
            fordercnt                   bigint,
            forderusernum               bigint,
            fpaycnt                     bigint,
            fpayusernum                 bigint,
            fincome                     decimal(20,2),
            freg_ordercnt               bigint,
            freg_orderusernum           bigint,
            freg_paycnt                 bigint,
            freg_payusernum             bigint,
            freg_income                 decimal(20,2),
            ffirst_ordercnt             bigint,
            ffirst_orderusernum         bigint,
            ffirst_paycnt               bigint,
            ffirst_payusernum           bigint,
            ffirst_income               decimal(20,2),
            fpm_name                    varchar(100)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        # 付费场景，明细统计
        hql = """
        insert into table analysis.scene_dim
        select a.a_fsk, a.a_fgamefsk, a.a_fname, a.fdisname
        from
        (
            select b.fgamefsk as a_fsk, b.fgamefsk as a_fgamefsk,
                nvl(a.fpay_scene,0) as a_fname, nvl(a.fpay_scene,0) fdisname
            from stage.user_payscene_mid a
            join analysis.bpid_platform_game_ver_map b
            on a.fbpid=b.fbpid
            where a.dt = "%(ld_begin)s"
            group by b.fgamefsk, nvl(a.fpay_scene,0)
        ) a
        left join analysis.scene_dim b
        on a.a_fgamefsk = b.fgamefsk and a.a_fname = b.fname
        where b.fgamefsk is null
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """
        insert overwrite table analysis.user_generate_order_fct partition(dt="%(stat_date)s")
        select "%(ld_begin)s" fdate,
               c.fgamefsk,
               c.fplatformfsk,
               c.fversionfsk,
               c.fterminalfsk,
               fgameparty_pname fpname,
               fgameparty_subname fsubname,
               fgameparty_anto fanto,
               nvl(fpay_scene, '其他') fpay_scene,
               fbankrupt fis_bankrupt,
               count(a.fplatform_uid) fordercnt,
               count(distinct a.fplatform_uid) forderusernum,
               count(case when fincome is not null then a.fplatform_uid end) fpaycnt,
               count(distinct case when fincome is not null then a.fplatform_uid end) fpayusernum,
               round(sum(fincome), 2) fincome,
               count(case when p.fuid is not null then a.fplatform_uid end) freg_ordercnt,
               count(distinct case when p.fuid is not null then  a.fplatform_uid end)                           freg_orderusernum,
               count(case when p.fuid is not null and fincome is not null then  a.fplatform_uid end)            freg_paycnt,
               count(distinct case when p.fuid is not null and fincome is not null then  a.fplatform_uid end)   freg_payusernum,
               round(sum(case when p.fuid is not null then fincome end), 2)                                     freg_income,
               count(case when m.fuid is not null then a.fplatform_uid end)                                     ffirst_ordercnt,
               count(distinct case when m.fuid is not null then  a.fplatform_uid end)                           ffirst_orderusernum,
               count(case when m.fuid is not null and fincome is not null then  a.fplatform_uid end)            ffirst_paycnt,
               count(distinct case when m.fuid is not null and fincome is not null then  a.fplatform_uid end)   ffirst_payusernum,
               round(sum(case when m.fuid is not null then fincome end), 2)                                     ffirst_income,
               fpm_name
          from stage.user_payscene_mid a
          left join stage.user_dim p
            on a.fbpid = p.fbpid
           and a.fuid = p.fuid
           and p.dt = '%(ld_begin)s'
          left join stage.pay_user_mid m
            on a.fbpid = m.fbpid
           and a.fplatform_uid = m.fplatform_uid
           and m.dt = '%(ld_begin)s'
          join analysis.bpid_platform_game_ver_map c
            on a.fbpid = c.fbpid
         where a.dt = "%(ld_begin)s"
         group by c.fgamefsk,
                  c.fplatformfsk,
                  c.fversionfsk,
                  c.fterminalfsk,
                  nvl(fpay_scene, '其他'),
                  fgameparty_pname,
                  fgameparty_subname,
                  fgameparty_anto,
                  fpm_name,
                  fbankrupt
        """ % self.hql_dict
        hql_list.append(hql)


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
    a = agg_user_generate_payscene(stat_date)
    a()
