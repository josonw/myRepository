#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_by_deferred_gamecoin(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求, 上个月1号
        create table if not exists analysis.by_deferred_gamecoin_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fin_all                 bigint,
            fout_all                bigint,
            fin_pay_about           bigint,
            fin_system              bigint,
            fin_system_pu           bigint,
            fin_system_npu          bigint,
            fout_taifei             bigint,
            fout_taifei_pu          bigint,
            fout_taifei_npu         bigint,
            fout_other              bigint,
            fout_other_pu           bigint,
            fout_other_npu          bigint,
            fbalance_all            bigint,
            fbalance_pu             bigint,
            fbalance_npu            bigint,
            fbalance_history        bigint,
            fbalance_history_pu     bigint,
            fbalance_history_npu    bigint,
            fsystem_fault           bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.dc_payuser_coin_stream
        (
            fsdate                      date,
            fedate                      date,
            fplatformfsk                bigint,
            fgamefsk                    bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fcointype                   varchar(50),
            fdirection                  varchar(50),
            ftype                       varchar(50),
            fnum                        bigint,
            fpayusernum                 bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.gameworld_balance_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fbalance                bigint,
            fpaybalance             bigint,
            fusernum                bigint,
            fpayusernum             bigint
        )
        partitioned by (dt date);
        create table if not exists analysis.dc_log_error_message
        (
            fsk                         bigint,
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            ftype                       varchar(50),
            fsolve_at                   string,
            flog_num                    bigint,
            flog_error_num              bigint,
            fdetail                     varchar(4000),
            fcomment                    varchar(4000),
            flog                        bigint,
            fstatus                     bigint,
            f1validation                varchar(50),
            f1validation_at             string,
            f2validation                varchar(50),
            f2validation_at             string,
            f3validation                varchar(50),
            f3validation_at             string,
            fsystem_fault               bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        alter table by_deferred_gamecoin_fct drop partition(dt = "%(ld_1month_ago_begin)s");
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        --  德州扑克(1396894)     收入相关act_id ('1','14','3','17','20','21','22','26','45','48','451','452','487','488') 保险箱('451',452)，玩家赠送('1',14) .牌局输赢('487',488)

        insert into table analysis.by_deferred_gamecoin_fct partition
        (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
             a.fgamefsk ,
               a.fplatformfsk ,
               fin_all ,
               fout_all ,
               fin_pay_about ,
               fin_system ,
               fin_system_pu ,
               fin_system - fin_system_pu fin_system_npu ,
               fout_taifei ,
               fout_taifei_pu ,
               fout_taifei - fout_taifei_pu fout_taifei_npu ,
               fout_other,
               fout_other_pu ,
               fout_other - fout_other_pu fout_other_npu ,
               nvl(fin_all,0) - nvl(fout_all,0) fbalance_all ,
               nvl(fin_pay_about,0) + nvl(fin_system_pu,0) - nvl(fout_taifei_pu,0) - nvl(fout_other_pu,0) fbalance_pu ,
               nvl(fin_system - fin_system_pu,0) - nvl(fout_taifei - fout_taifei_pu,0) - nvl(fout_other - fout_other_pu,0) fbalance_npu ,
               fbalance fbalance_history ,
               fpaybalance fbalance_history_pu ,
               fbalance - fpaybalance fbalance_history_npu,
               0 fsystem_fault
        from (
        select fgamefsk, fplatformfsk, fbalance, fpaybalance from analysis.gameworld_balance_fct
        where dt = date_add('%(ld_month_begin)s', -1)
        and fgamefsk = 1396894
        ) a left join (
        select fgamefsk, fplatformfsk,
               nvl(sum(case when fdirection=1 and ftype not in ('1','14','451','452','487','488')  then fnum end ),0) fin_all,
               nvl(sum(case when fdirection=2 and ftype not in ('1','14','451','452','487','488') then fnum end ),0) fout_all,
               nvl(sum(case when fdirection=1 and ftype in ('3','17','20','21','22','26','45',48) then fnum end ),0) fin_pay_about,
               nvl(sum(case when fdirection=1 and ftype not in ('1','14','3','17','20','21','22','26','45','48','451','452','487','488') then fnum end ),0) fin_system,
               nvl(sum(case when fdirection=1 and ftype not in ('1','14','3','17','20','21','22','26','45','48','451','452','487','488') then fpayusernum end ),0) fin_system_pu,
               nvl(sum(case when fdirection=2 and ftype=27 then fnum else 0 end ),0) fout_taifei,
               nvl(sum(case when fdirection=2 and ftype=27 then fpayusernum else 0 end ),0) fout_taifei_pu,
               nvl(sum(case when fdirection=2 and ftype not in ('1','14','27','451','452','487','488') then fnum end ),0) fout_other,
               nvl(sum(case when fdirection=2 and ftype not in ('1','14','27','451','452','487','488') then fpayusernum end ),0) fout_other_pu
        from analysis.dc_payuser_coin_stream a
        where a.dt = '%(ld_1month_ago_begin)s'
          and fgamefsk = 1396894
        group by fgamefsk, fplatformfsk
        ) b on a.fgamefsk=b.fgamefsk
        and a.fplatformfsk=b.fplatformfsk
        order by a.fgamefsk, a.fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        --  斗地主(1396895)      收入相关act_id ('11','15','200','204'), 牌局输赢(1000)
        insert into table analysis.by_deferred_gamecoin_fct partition
        (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
             a.fgamefsk ,
               a.fplatformfsk ,
               fin_all ,
               fout_all ,
               fin_pay_about ,
               fin_system ,
               fin_system_pu ,
               fin_system - fin_system_pu fin_system_npu ,
               fout_taifei ,
               fout_taifei_pu ,
               fout_taifei - fout_taifei_pu fout_taifei_npu ,
               fout_other,
               fout_other_pu ,
               fout_other - fout_other_pu fout_other_npu ,
               nvl(fin_all,0) - nvl(fout_all,0) fbalance_all ,
               nvl(fin_pay_about,0) + nvl(fin_system_pu,0) - nvl(fout_taifei_pu,0) - nvl(fout_other_pu,0) fbalance_pu ,
               nvl(fin_system - fin_system_pu,0) - nvl(fout_taifei - fout_taifei_pu,0) - nvl(fout_other - fout_other_pu,0) fbalance_npu ,
               fbalance fbalance_history ,
               fpaybalance fbalance_history_pu ,
               fbalance - fpaybalance fbalance_history_npu,
               0 fsystem_fault
        from (
        select fgamefsk, fplatformfsk, fbalance, fpaybalance from analysis.gameworld_balance_fct
        where dt = date_add('%(ld_month_begin)s', -1)
        and fgamefsk = 1396895
        ) a left join (
        select fgamefsk, fplatformfsk,
               nvl(sum(case when fdirection=1  then fnum end ),0) fin_all,
               nvl(sum(case when fdirection=2  then fnum end ),0) fout_all,
               nvl(sum(case when fdirection=1 and ftype in ('11','15','200','204') then fnum end ),0) fin_pay_about,
               nvl(sum(case when fdirection=1 and ftype not in ('11','15','200','204') then fnum end ),0) fin_system,
               nvl(sum(case when fdirection=1 and ftype not in ('11','15', '200', '204') then fpayusernum end ),0) fin_system_pu,
               nvl(sum(case when fdirection=2 and ftype=27 then fnum else 0 end ),0) fout_taifei,
               nvl(sum(case when fdirection=2 and ftype=27 then fpayusernum else 0 end ),0) fout_taifei_pu,
               nvl(sum(case when fdirection=2 and ftype not in ('11','15','200','204') then fnum end ),0) fout_other,
               nvl(sum(case when fdirection=2 and ftype not in ('11','15','200','204') then fpayusernum end ),0) fout_other_pu
        from analysis.dc_payuser_coin_stream a
        where a.dt = '%(ld_1month_ago_begin)s'
          and fgamefsk = 1396895
        group by fgamefsk, fplatformfsk
        ) b on a.fgamefsk=b.fgamefsk
        and a.fplatformfsk=b.fplatformfsk
        order by a.fgamefsk, a.fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        --  锄大地(1396898)  收入相关act_id ('15','200') ,   牌局输赢(1000)
        insert into table analysis.by_deferred_gamecoin_fct partition
        (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
             a.fgamefsk ,
               a.fplatformfsk ,
               fin_all ,
               fout_all ,
               fin_pay_about ,
               fin_system ,
               fin_system_pu ,
               fin_system - fin_system_pu fin_system_npu ,
               fout_taifei ,
               fout_taifei_pu ,
               fout_taifei - fout_taifei_pu fout_taifei_npu ,
               fout_other,
               fout_other_pu ,
               fout_other - fout_other_pu fout_other_npu ,
               nvl(fin_all,0) - nvl(fout_all,0) fbalance_all ,
               nvl(fin_pay_about,0) + nvl(fin_system_pu,0) - nvl(fout_taifei_pu,0) - nvl(fout_other_pu,0) fbalance_pu ,
               nvl(fin_system - fin_system_pu,0) - nvl(fout_taifei - fout_taifei_pu,0) - nvl(fout_other - fout_other_pu,0) fbalance_npu ,
               fbalance fbalance_history ,
               fpaybalance fbalance_history_pu ,
               fbalance - fpaybalance fbalance_history_npu,
               0 fsystem_fault
        from (
        select fgamefsk, fplatformfsk, fbalance, fpaybalance from analysis.gameworld_balance_fct
        where dt = date_add('%(ld_month_begin)s', -1)
        and fgamefsk = 1396898
        ) a left join (
        select fgamefsk, fplatformfsk,
               nvl(sum(case when fdirection=1  then fnum end ),0) fin_all,
               nvl(sum(case when fdirection=2  then fnum end ),0) fout_all,
               nvl(sum(case when fdirection=1 and ftype in ('15','200') then fnum end ),0) fin_pay_about,
               nvl(sum(case when fdirection=1 and ftype not in ('15','200') then fnum end ),0) fin_system,
               nvl(sum(case when fdirection=1 and ftype not in ('15','200') then fpayusernum end ),0) fin_system_pu,
               nvl(sum(case when fdirection=2 and ftype=27 then fnum else 0 end ),0) fout_taifei,
               nvl(sum(case when fdirection=2 and ftype=27 then fpayusernum else 0 end ),0) fout_taifei_pu,
               nvl(sum(case when fdirection=2 and ftype not in ('15','200') then fnum end ),0) fout_other,
               nvl(sum(case when fdirection=2 and ftype not in ('15','200') then fpayusernum end ),0) fout_other_pu
        from analysis.dc_payuser_coin_stream a
        where a.dt = '%(ld_1month_ago_begin)s'
          and fgamefsk = 1396898
        group by fgamefsk, fplatformfsk
        ) b on a.fgamefsk=b.fgamefsk
        and a.fplatformfsk=b.fplatformfsk
        order by a.fgamefsk, a.fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        --  四人斗地主(1396896)    收入相关act_id ('997','63',3) , 牌局输赢(500)
        insert into table analysis.by_deferred_gamecoin_fct partition
        (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
             a.fgamefsk ,
               a.fplatformfsk ,
               fin_all ,
               fout_all ,
               fin_pay_about ,
               fin_system ,
               fin_system_pu ,
               fin_system - fin_system_pu fin_system_npu ,
               fout_taifei ,
               fout_taifei_pu ,
               fout_taifei - fout_taifei_pu fout_taifei_npu ,
               fout_other,
               fout_other_pu ,
               fout_other - fout_other_pu fout_other_npu ,
               nvl(fin_all,0) - nvl(fout_all,0) fbalance_all ,
               nvl(fin_pay_about,0) + nvl(fin_system_pu,0) - nvl(fout_taifei_pu,0) - nvl(fout_other_pu,0) fbalance_pu ,
               nvl(fin_system - fin_system_pu,0) - nvl(fout_taifei - fout_taifei_pu,0) - nvl(fout_other - fout_other_pu,0) fbalance_npu ,
               fbalance fbalance_history ,
               fpaybalance fbalance_history_pu ,
               fbalance - fpaybalance fbalance_history_npu,
               0 fsystem_fault
        from (
        select fgamefsk, fplatformfsk, fbalance, fpaybalance from analysis.gameworld_balance_fct
        where dt = date_add('%(ld_month_begin)s', -1)
        and fgamefsk = 1396896
        ) a left join (
        select fgamefsk, fplatformfsk,
               nvl(sum(case when fdirection=1  then fnum end ),0) fin_all,
               nvl(sum(case when fdirection=2  then fnum end ),0) fout_all,
               nvl(sum(case when fdirection=1 and ftype in ('997','63','3') then fnum end ),0) fin_pay_about,
               nvl(sum(case when fdirection=1 and ftype not in ('997','63','3') then fnum end ),0) fin_system,
               nvl(sum(case when fdirection=1 and ftype not in ('997','63','3') then fpayusernum end ),0) fin_system_pu,
               nvl(sum(case when fdirection=2 and ftype=27 then fnum else 0 end ),0) fout_taifei,
               nvl(sum(case when fdirection=2 and ftype=27 then fpayusernum else 0 end ),0) fout_taifei_pu,
               nvl(sum(case when fdirection=2 and ftype not in ('997','63','3') then fnum end ),0) fout_other,
               nvl(sum(case when fdirection=2 and ftype not in ('997','63','3') then fpayusernum end ),0) fout_other_pu
        from analysis.dc_payuser_coin_stream a
        where a.dt = '%(ld_1month_ago_begin)s'
          and fgamefsk = 1396896
        group by fgamefsk, fplatformfsk
        ) b on a.fgamefsk=b.fgamefsk
        and a.fplatformfsk=b.fplatformfsk
        order by a.fgamefsk, a.fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        --  麻将 (1396902)  收入相关act_id ('21','104',70,'208') ,    牌局输赢('3',4)
        insert into table analysis.by_deferred_gamecoin_fct partition
        (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
             a.fgamefsk ,
               a.fplatformfsk ,
               fin_all ,
               fout_all ,
               fin_pay_about ,
               fin_system ,
               fin_system_pu ,
               fin_system - fin_system_pu fin_system_npu ,
               fout_taifei ,
               fout_taifei_pu ,
               fout_taifei - fout_taifei_pu fout_taifei_npu ,
               fout_other,
               fout_other_pu ,
               fout_other - fout_other_pu fout_other_npu ,
               nvl(fin_all,0) - nvl(fout_all,0) fbalance_all ,
               nvl(fin_pay_about,0) + nvl(fin_system_pu,0) - nvl(fout_taifei_pu,0) - nvl(fout_other_pu,0) fbalance_pu ,
               nvl(fin_system - fin_system_pu,0) - nvl(fout_taifei - fout_taifei_pu,0) - nvl(fout_other - fout_other_pu,0) fbalance_npu ,
               fbalance fbalance_history ,
               fpaybalance fbalance_history_pu ,
               fbalance - fpaybalance fbalance_history_npu,
               0 fsystem_fault
        from (
        select fgamefsk, fplatformfsk, fbalance, fpaybalance from analysis.gameworld_balance_fct
        where dt = date_add('%(ld_month_begin)s', -1)
        and fgamefsk = 1396902
        ) a left join (
        select fgamefsk, fplatformfsk,
               nvl(sum(case when fdirection=1  then fnum end ),0) fin_all,
               nvl(sum(case when fdirection=2  then fnum end ),0) fout_all,
               nvl(sum(case when fdirection=1 and ftype in ('21','104','70','208') then fnum end ),0) fin_pay_about,
               nvl(sum(case when fdirection=1 and ftype not in ('21','104','70','3','4','208') then fnum end ),0) fin_system,
               nvl(sum(case when fdirection=1 and ftype not in ('21','104','70','3','4','208') then fpayusernum end ),0) fin_system_pu,
               nvl(sum(case when fdirection=2 and ftype=1 then fnum else 0 end ),0) fout_taifei,
               nvl(sum(case when fdirection=2 and ftype=1 then fpayusernum else 0 end ),0) fout_taifei_pu,
               nvl(sum(case when fdirection=2 and ftype not in ('1', '21','104','70','3','4','208') then fnum end ),0) fout_other,
               nvl(sum(case when fdirection=2 and ftype not in ('1', '21','104','70','3','4','208') then fpayusernum end ),0) fout_other_pu
        from analysis.dc_payuser_coin_stream a
        where a.dt = '%(ld_1month_ago_begin)s'
          and fgamefsk = 1396902
        group by fgamefsk, fplatformfsk
        ) b on a.fgamefsk=b.fgamefsk
        and a.fplatformfsk=b.fplatformfsk
        order by a.fgamefsk, a.fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        --  其他游戏
        insert into table analysis.by_deferred_gamecoin_fct partition
        (dt = "%(ld_1month_ago_begin)s")
          select '%(ld_1month_ago_begin)s' fdate,
             a.fgamefsk ,
               a.fplatformfsk ,
               fin_all ,
               fout_all ,
               fin_pay_about ,
               fin_system ,
               fin_system_pu ,
               fin_system - fin_system_pu fin_system_npu ,
               fout_taifei ,
               fout_taifei_pu ,
               fout_taifei - fout_taifei_pu fout_taifei_npu ,
               fout_other,
               fout_other_pu ,
               fout_other - fout_other_pu fout_other_npu ,
               nvl(fin_all,0) - nvl(fout_all,0) fbalance_all ,
               nvl(fin_pay_about,0) + nvl(fin_system_pu,0) - nvl(fout_taifei_pu,0) - nvl(fout_other_pu,0) fbalance_pu ,
               nvl(fin_system - fin_system_pu,0) - nvl(fout_taifei - fout_taifei_pu,0) - nvl(fout_other - fout_other_pu,0) fbalance_npu ,
               fbalance fbalance_history ,
               fpaybalance fbalance_history_pu ,
               fbalance - fpaybalance fbalance_history_npu,
               0 fsystem_fault
        from (
        select fgamefsk, fplatformfsk, fbalance, fpaybalance from analysis.gameworld_balance_fct
        where dt = date_add('%(ld_month_begin)s', -1)
        and fgamefsk not in (1396894, 1396895, 1396898, 1396896, 1396902)
        ) a left join (
        select a.fgamefsk, fplatformfsk,
               nvl(sum(case when a.fdirection=1 then fnum end ),0) fin_all,
               nvl(sum(case when a.fdirection=2 then fnum end ),0) fout_all,
               nvl(sum(case when a.fdirection=1 and nvl(b.fis_pay,0) = 1 then fnum end ),0) fin_pay_about,
               nvl(sum(case when a.fdirection=1 and nvl(b.fis_pay,0) <> 1 then fnum end ),0) fin_system,
               nvl(sum(case when a.fdirection=1 and nvl(b.fis_pay,0) <> 1 then fpayusernum end ),0) fin_system_pu,
               nvl(sum(case when a.fdirection=2 and nvl(b.fis_taifei,0) = 1 then fnum else 0 end ),0) fout_taifei,
               nvl(sum(case when a.fdirection=2 and nvl(b.fis_taifei,0) = 1 then fpayusernum else 0 end ),0) fout_taifei_pu,
               nvl(sum(case when a.fdirection=2 and nvl(b.fis_taifei,0) <> 1 then fnum end ),0) fout_other,
               nvl(sum(case when a.fdirection=2 and nvl(b.fis_taifei,0) <> 1 then fpayusernum end ),0) fout_other_pu
         from analysis.dc_payuser_coin_stream a
        left join analysis.game_coin_type_dim b
          on a.fgamefsk = b.fgamefsk
         and a.fcointype = b.fcointype
         and (case when a.fdirection = 1 then 'IN' when a.fdirection = 2 then 'OUT' else 'err' end) = b.fdirection
         and a.ftype = b.ftype
        where a.dt = '%(ld_1month_ago_begin)s'
          and a.fgamefsk not in (1396894, 1396895, 1396898, 1396896, 1396902)
          and b.fis_circulate != '1'
        group by a.fgamefsk, fplatformfsk
        ) b on a.fgamefsk=b.fgamefsk
        and a.fplatformfsk=b.fplatformfsk
        order by a.fgamefsk, a.fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 处理系统故障值
        insert into table analysis.by_deferred_gamecoin_fct  partition
        (dt = "%(ld_1month_ago_begin)s")
        select
        '%(ld_1month_ago_begin)s' fdate,
        fgamefsk,
        fplatformfsk,
        0 fin_all,
        0 fout_all,
        0 fin_pay_about,
        0 fin_system,
        0 fin_system_pu,
        0 fin_system_npu,
        0 fout_taifei,
        0 fout_taifei_pu,
        0 fout_taifei_npu,
        0 fout_other,
        0 fout_other_pu,
        0 fout_other_npu,
        0 fbalance_all,
        0 fbalance_pu,
        0 fbalance_npu,
        0 fbalance_history,
        0 fbalance_history_pu,
        0 fbalance_history_npu,
        sum(fsystem_fault) fsystem_fault
         from analysis.dc_log_error_message
        where fdate >= '%(ld_1month_ago_begin)s'
          and fdate < '%(ld_month_begin)s'
        group by fgamefsk, fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.by_deferred_gamecoin_fct partition
            (dt = "%(ld_1month_ago_begin)s")
        select fdate,
        fgamefsk,
        fplatformfsk,
        max(fin_all)                    fin_all,
        max(fout_all)                   fout_all,
        max(fin_pay_about)              fin_pay_about,
        max(fin_system)                 fin_system,
        max(fin_system_pu)              fin_system_pu,
        max(fin_system_npu)             fin_system_npu,
        max(fout_taifei)                fout_taifei,
        max(fout_taifei_pu)             fout_taifei_pu,
        max(fout_taifei_npu)            fout_taifei_npu,
        max(fout_other)                 fout_other,
        max(fout_other_pu)              fout_other_pu,
        max(fout_other_npu)             fout_other_npu,
        max(fbalance_all)               fbalance_all,
        max(fbalance_pu)                fbalance_pu,
        max(fbalance_npu)               fbalance_npu,
        max(fbalance_history)           fbalance_history,
        max(fbalance_history_pu)        fbalance_history_pu,
        max(fbalance_history_npu)       fbalance_history_npu,
        max(fsystem_fault)              fsystem_fault
        from analysis.by_deferred_gamecoin_fct
        where dt = "%(ld_1month_ago_begin)s"
        group by fdate,
                fgamefsk,
                fplatformfsk
        """ % self.hql_dict
        hql_list.append( hql )


        # 只有月初日期才会执行
        if self.hql_dict.get('stat_date', '').endswith('-01'):
            result = self.exe_hql_list(hql_list)
        else:
            result = ''
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
    a = agg_by_deferred_gamecoin(stat_date)
    a()
