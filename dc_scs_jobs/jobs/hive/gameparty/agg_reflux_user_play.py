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


class agg_reflux_user_play(BaseStat):
    """7日回流用户，玩牌用户
    """
    def create_tab(self):
        hql = """create table if not exists dcnew.reflux_user_play
                (
                fdate                      date,
                fgamefsk                   bigint,
                fplatformfsk               bigint,
                fhallfsk                   bigint,
                fsubgamefsk                bigint,
                fterminaltypefsk           bigint,
                fversionfsk                bigint,
                fchannelcode               bigint,
                fretcnt                    bigint,
                fretplaycnt                bigint
                )
                partitioned by (dt date)
                """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        alias_dic = {'bpid_tbl_alias':'bgm.','src_tbl_alias':'src.'}
        temp = dict(alias_dic,**sql_const.const_dict())
        query = {
            'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],
            'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
            'select_field_str':sql_const.multi_div_select_str(isreport=0)%temp ,
            'group_by':sql_const.extend_groupset(sql_const.GROUPSET) % alias_dic }

        query.update(sql_const.const_dict())
        query.update(PublicFunc.date_define(self.stat_date))

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql ="""
        insert overwrite table dcnew.reflux_user_play
        partition (dt = '%(ld_daybegin)s')
        select "%(ld_daybegin)s" fdate,
               fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               count(distinct fuid) fretcnt,
               count(distinct case when fparty_num > 0 then fuid end) fretplaycnt
        from dim.user_reflux_array a
       where a.dt = '%(ld_daybegin)s'
         and a.freflux = 7
         and a.freflux_type='cycle'
        group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_reflux_user_play(statDate)
a()
