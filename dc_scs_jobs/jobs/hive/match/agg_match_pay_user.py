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


class agg_match_pay_user(BaseStat):

    def create_tab(self):
        hql = """create table if not exists dcnew.match_pay_user
            (
            fdate                      date,
            fgamefsk                   bigint,
            fplatformfsk               bigint,
            fhallfsk                   bigint,
            fsubgamefsk                bigint,
            fterminaltypefsk           bigint,
            fversionfsk                bigint,
            fchannelcode               bigint,

            fpayusernum                bigint,        --付费人数
            fpaycnt                    bigint,        --付费次数
            fincome                    decimal(20,2), --付费金额
            fumaxpcnt                  bigint,        --单用户最多付费次数
            fumaxincome                decimal(20,2)  --用户单次最高付费金额
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)

    def stat(self):
        hql_list =[]
        alias_dic = {'bpid_tbl_alias':'bgm.','src_tbl_alias':'src.'}
        query = { 'statdate':self.stat_date,'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],\
            'group_by':sql_const.HQL_GROUP_BY_ALL % alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,\
            'null_str_report':sql_const.NULL_STR_REPORT,'null_int_report':sql_const.NULL_INT_REPORT}

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.match_pay_user
        partition(dt = '%(statdate)s')
          select /*+ MAPJOIN(%(bpid_tbl_alias)s) */ '%(statdate)s' fdate,
                %(bpid_tbl_alias)s.fgamefsk,
                coalesce(%(bpid_tbl_alias)s.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(%(bpid_tbl_alias)s.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(%(src_tbl_alias)s.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(%(bpid_tbl_alias)s.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(%(bpid_tbl_alias)s.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(%(src_tbl_alias)s.fchannel_code,%(null_int_group_rule)d) fchannelcode,
                count(distinct fplatform_uid) fpayusernum,
                sum(fpaycnt) fpaycnt,
                sum(dip) fincome,
                max(fpaycnt) fumaxpcnt,
                max(fumaxincome) fumaxincome

            from( select ps.fbpid,
                    coalesce(mcp.fgame_id,%(null_int_report)d) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    ps.fplatform_uid,
                    ps.fuid,
                    ps.fpaycnt,
                    ps.dip,
                    ps.fumaxincome
                from (select  a.fbpid,
                                   a.fplatform_uid,
                                   e.fuid,
                                   sum(round(fcoins_num * frate, 2)) dip,
                                   count(1) fpaycnt,
                                   max(round(fcoins_num * frate, 2)) fumaxincome
                              from stage.payment_stream_stg a
                              left join stage.pay_user_mid e
                                on e.fbpid = a.fbpid
                               and e.fplatform_uid = a.fplatform_uid
                             where a.dt = "%(statdate)s"
                             group by a.fbpid, a.fplatform_uid,e.fuid) ps

                join ( select fbpid, fuid, max(fchannel_code) fchannel_code
                       from stage.user_gameparty_stg where dt='%(statdate)s' and  coalesce(fmatch_id,'0')<>'0'
                       group by fbpid, fuid
                      ) ug

                on ug.fbpid=ps.fbpid and ug.fuid=ps.fuid

                left join analysis.marketing_channel_pkg_info mcp
                on ug.fchannel_code = mcp.fid

                group by ps.fbpid,

                    coalesce(mcp.fgame_id,%(null_int_report)d),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    ps.fplatform_uid,
                    ps.fuid,
                    ps.fpaycnt,
                    ps.dip,
                    ps.fumaxincome

                ) %(src_tbl_alias)s

          join dim.bpid_map %(bpid_tbl_alias)s
          on %(src_tbl_alias)s.fbpid = %(bpid_tbl_alias)s.fbpid
          %(group_by)s

        """% query


        hql_list.append(hql)

        result = self.exe_hql_list(hql_list)
        return result


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
a = agg_match_pay_user(statDate, eid)
a()
