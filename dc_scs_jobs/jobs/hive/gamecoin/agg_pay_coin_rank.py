#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/service' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import sql_const



alias_dic = {'bpid_tbl_alias':'e.','src_tbl_alias':'c.'}



class agg_pay_coin_rank(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_coin_rank
            (
            fdate              date,
            fgamefsk           bigint,
            fplatformfsk       bigint,
            fhallfsk           bigint,
            fsubgamefsk        bigint,
            fterminaltypefsk   bigint,
            fversionfsk        bigint,
            fchannelcode       bigint,
            fuid               bigint,
            fplatform_uid      varchar(50),
            fgame_platform     varchar(80),
            fp_type            int,
            fpaycnt            int,
            dip                decimal(20,2),
            fpcoin_nums        bigint,
            fplaynum           int,
            fgc_in             bigint,
            fgc_out            bigint
            )
            partitioned by (dt string)
            """

        res = self.hq.exe_sql(hql)


    def stat(self):
        hql_list = []

        query = { 'statdate':self.stat_date,"num_date": self.stat_date.replace("-", ""),
            'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_str_report':sql_const.NULL_STR_REPORT,'null_int_report':sql_const.NULL_INT_REPORT}


        hql = """insert overwrite table dcnew.pay_coin_rank
                 partition(dt = '%(statdate)s')
                select /*+ MAPJOIN(%(bpid_tbl_alias)s) */ '%(statdate)s' fdate,
                %(bpid_tbl_alias)s.fgamefsk,
                coalesce(%(bpid_tbl_alias)s.fplatformfsk,%(null_int_report)d) fplatformfsk,
                coalesce(%(bpid_tbl_alias)s.fhallfsk,%(null_int_report)d) fhallfsk,
                coalesce(%(src_tbl_alias)s.fgame_id,%(null_int_report)d) fsubgamefsk,
                coalesce(%(bpid_tbl_alias)s.fterminaltypefsk,%(null_int_report)d) fterminaltypefsk,
                coalesce(%(bpid_tbl_alias)s.fversionfsk,%(null_int_report)d) fversionfsk,
                coalesce(%(src_tbl_alias)s.fchannel_code,%(null_int_report)d) fchannelcode,

                a.fuid, a.fplatform_uid,
                concat_ws('-', fgamename, fplatformname) fgame_platform,
                a.fp_type fp_type,
                sum(a.fpaycnt) fpaycnt,
                sum(a.dip) dip,
                sum(a.fp_num) fpcoin_nums,
                sum(coalesce(b.fparty_num,0)) fplaynum,
                sum(coalesce(case when a.fp_type = 1 then d.fwin_amt
                                  when a.fp_type = 2 then c.fwin_amt else 0 end ,0)) fgc_in,
                sum(coalesce(case when a.fp_type = 1 then d.flose_amt
                                  when a.fp_type = 2 then c.flose_amt else 0 end ,0)) fgc_out
                from
                 (
                select a.dt,a.fbpid, coalesce(e.fuid,a.fuid) fuid, a.fplatform_uid,
                        count(distinct a.forder_id) fpaycnt, fp_type, sum(round(a.fcoins_num * a.frate, 2)) dip, sum(a.fp_num) fp_num

                    FROM stage.payment_stream_stg a
                    left join stage.pay_user_mid e
                    on e.fbpid = a.fbpid
                   and e.fplatform_uid = a.fplatform_uid
                 where a.dt = "%(statdate)s"

                group by a.dt, a.fbpid, a.fplatform_uid, coalesce(e.fuid,a.fuid) , fp_type
                 ) a

                left join ( select fbpid, fuid,
                                   count(distinct concat_ws('0', finning_id, ftbl_id) ) fparty_num,
                                   case when fcoins_type=0 then 2
                                        when fcoins_type=1 then 1 else 2 end fp_type

                            from stage.user_gameparty_stg where dt = "%(statdate)s"
                            group by fbpid, fuid,
                                     case when fcoins_type=0 then 2
                                          when fcoins_type=1 then 1 else 2 end ) b

                on a.fbpid=b.fbpid and a.fuid=b.fuid and a.fp_type =b.fp_type

                left join ( select fbpid, fuid, fgame_id, fchannel_code,
                                   sum(fadd_amt) fwin_amt, sum(fremove_amt) flose_amt
                            from dim.user_gamecoin_stream where dt = "%(statdate)s"
                            group by fbpid, fuid, fgame_id, fchannel_code) c
                on a.fbpid=c.fbpid and a.fuid=c.fuid

                left join ( select fbpid, fuid,
                                   sum(fadd_amt) fwin_amt, sum(fremove_amt) flose_amt
                            from dim.user_bycoin_stream where dt = "%(statdate)s"
                            group by fbpid, fuid) d
                on a.fbpid=d.fbpid and a.fuid=d.fuid

                join dim.bpid_map e
                on a.fbpid=e.fbpid
                group by %(bpid_tbl_alias)s.fgamefsk,%(bpid_tbl_alias)s.fgamename,%(bpid_tbl_alias)s.fplatformfsk,%(bpid_tbl_alias)s.fplatformname,
                %(bpid_tbl_alias)s.fhallfsk,%(src_tbl_alias)s.fgame_id,%(bpid_tbl_alias)s.fterminaltypefsk,
                %(bpid_tbl_alias)s.fversionfsk,%(src_tbl_alias)s.fchannel_code,
                a.fuid, a.fplatform_uid, a.fp_type
              """%query


        res = self.hq.exe_sql(hql)
        if res != 0:
            return res



# 愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else:
    args = sys.argv[1].split(',')
    statDate = args[0]


# 生成统计实例
a = agg_pay_coin_rank(statDate, eid)
a()
