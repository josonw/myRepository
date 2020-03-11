# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_bycoin_detail(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.bycoin_detail (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fdate_range         string     comment '数据周期：day',
               fpay_flag           int        comment '是否付费:0否1是',
               fgcc_by_num         bigint     comment '博雅币消耗总数',
               fgci_by_num         bigint     comment '博雅币发放总数',
               fgbl_by_num         bigint     comment '博雅币流水结余',
               fuserbl_by_num      bigint     comment '博雅币用户结余'
               )comment '博雅币明细表'
               partitioned by(dt date)
        location '/dw/dcnew/bycoin_detail'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fdate_range','fpay_flag'],
                        'groups':[[1,1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--
        drop table if exists work.bycoin_detail_tmp_b_%(statdatenum)s;
        create table work.bycoin_detail_tmp_b_%(statdatenum)s as
                select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       coalesce(fgame_id,%(null_int_report)d) fgame_id,
                       coalesce(fchannel_code,%(null_int_report)d) fchannel_code,
                       'day' fdate_range,
                       0 fpay_flag,
                       sum(user_bycoins_num) fact_num
                  from dim.user_bycoin_balance p
                  left join dim.user_bycoin_balance_day a
                    on p.fbpid = a.fbpid
                   and p.fuid = a.fuid
                   and a.dt = "%(statdate)s"
                  join dim.bpid_map c
                    on p.fbpid=c.fbpid
                 where p.dt='%(statdate)s'
                   and p.user_bycoins_num >= 0
                 group by c.fgamefsk,
                          c.fplatformfsk,
                          c.fhallfsk,
                          c.fterminaltypefsk,
                          c.fversionfsk,
                          c.hallmode,
                          coalesce(fgame_id,%(null_int_report)d),
                          coalesce(fchannel_code,%(null_int_report)d)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fdate_range,
                       fpay_flag,
                       0 fgcc_by_num,
                       0 fgci_by_num,
                       0 fgbl_by_num,
                       sum(fact_num) fuserbl_by_num
                  from work.bycoin_detail_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fdate_range,
                       fpay_flag
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        drop table if exists work.bycoin_detail_tmp_%(statdatenum)s;
        create table work.bycoin_detail_tmp_%(statdatenum)s as
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res



        hql = """
        insert overwrite table dcnew.bycoin_detail
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate,
                fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fdate_range,
                fpay_flag,
                sum(fgcc_by_num) fgcc_by_num,
                sum(fgci_by_num) fgci_by_num,
                sum(fgci_by_num)-sum(fgcc_by_num) fgbl_by_num,
                sum(fuserbl_by_num) fuserbl_by_num
          from (select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fdate_range,
                       fpay_flag,
                       0 fgcc_by_num,
                       0 fgci_by_num,
                       fuserbl_by_num
                  from work.bycoin_detail_tmp_%(statdatenum)s gs
                union all
               select  fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk fgame_id,fterminaltypefsk,fversionfsk, fchannelcode,
                       'day' fdate_range,
                       fpay_flag,
                       case when fdirection = 'OUT' then fby_num end fgcc_by_num,
                       case when fdirection = 'IN' then fby_num end fgci_by_num,
                       0 fuserbl_by_num
                  from dcnew.bycoin_general gs
                 where fdate='%(statdate)s') t
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fdate_range,
                       fpay_flag
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bycoin_detail_tmp_b_%(statdatenum)s;
                 drop table if exists work.bycoin_detail_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_bycoin_detail(sys.argv[1:])
a()
