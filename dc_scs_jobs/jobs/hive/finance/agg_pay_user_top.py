# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_user_top(BaseStatModel):
    """ 新后台付费用户top100"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_top
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fplatform_uid               string,
            fuid                        bigint,
            fpaycnt                     int,
            dip                         decimal(20,2),
            flognum                     int,
            fplaynum                    int,
            fgc_in                      bigint,
            fgc_out                     bigint,
            flogin_at        bigint         comment '登录时间',
            fgamecoin         bigint         comment '金币结余'

        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql = """
            drop table if exists work.pay_user_top_%(statdatenum)s;
          create table work.pay_user_top_%(statdatenum)s as
        select fgamefsk, fplatformfsk, fuid, flogin_at, user_gamecoins
          from (select fgamefsk
                       ,fplatformfsk
                       ,fuid
                       ,flogin_at
                       ,user_gamecoins
                       ,row_number() over(partition by fgamefsk,fplatformfsk,fuid order by flogin_at desc) row_num
                  from dim.user_login_last t1
                 where dt = '%(statdate)s') t
         where row_num = 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.pay_user_top
        partition (dt = "%(statdate)s")
         select t1.fdate
                ,t1.fgamefsk
                ,t1.fplatformfsk
                ,t1.fhallfsk
                ,t1.fsubgamefsk
                ,t1.fterminaltypefsk
                ,t1.fversionfsk
                ,t1.fchannelcode
                ,t1.fplatform_uid
                ,t1.fuid
                ,t1.fpaycnt
                ,t1.dip
                ,t1.flognum
                ,t1.fplaynum
                ,t1.fgc_in
                ,t1.fgc_out
                ,t2.flogin_at
                ,t2.user_gamecoins
           from (select '%(statdate)s' fdate
                        ,d.fgamefsk
                        ,coalesce(d.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                        ,coalesce(d.fhallfsk,%(null_int_group_rule)d) fhallfsk
                        ,%(null_int_group_rule)d fsubgamefsk
                        ,coalesce(d.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                        ,coalesce(d.fversionfsk,%(null_int_group_rule)d) fversionfsk
                        ,%(null_int_group_rule)d fchannelcode  --去掉渠道维度，修改渠道默认-21379 0516
                        ,a.fplatform_uid
                        ,a.fuid
                        ,max(a.fpay_cnt) fpaycnt
                        ,max(a.ftotal_usd_amt) dip
                        ,max(b.flogin_cnt) flognum
                        ,max(b.fparty_num) fplaynum
                        ,max(c.fgc_in) fgc_in
                        ,max(c.fgc_out) fgc_out
                   from dim.user_pay_day a
                   left join (select fbpid,fuid,
                                     sum(flogin_cnt) flogin_cnt,
                                     sum(fparty_num) fparty_num
                                from dim.user_act
                               where dt = '%(statdate)s'
                               group by fbpid,fuid
                               ) b
                     on a.fbpid=b.fbpid
                    and a.fuid=b.fuid
                   left join (select fbpid,fuid,
                                    sum(case fact_type when 1 then fnum end) fgc_in,
                                    sum(case fact_type when 2 then abs(fnum) end) fgc_out
                                    from dim.user_gamecoin_stream_day where dt = '%(statdate)s'
                                    group by fbpid, fuid
                              ) c
                     on a.fbpid=c.fbpid
                    and a.fuid=c.fuid
                   join dim.bpid_map d
                     on a.fbpid=d.fbpid
                  where a.dt = '%(statdate)s'
                  group by d.fgamefsk,d.fplatformfsk,d.fhallfsk,d.fterminaltypefsk,d.fversionfsk,a.fchannel_code,
                        a.fplatform_uid,a.fuid
                ) t1
           left join work.pay_user_top_%(statdatenum)s t2
             on t1.fgamefsk=t2.fgamefsk
            and t1.fplatformfsk=t2.fplatformfsk
            and t1.fuid=t2.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        hql = """
        drop table if exists work.pay_user_top_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = agg_pay_user_top(sys.argv[1:])
a()
