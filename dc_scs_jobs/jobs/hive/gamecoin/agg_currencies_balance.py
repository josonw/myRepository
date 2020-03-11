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


class agg_currencies_balance(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.currencies_balance (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fcointype           varchar(10)  comment '货币类型',
               fpay_flag           int          comment '是否付费:0否1是',
               fgcc_coin_num       bigint       comment '货币消耗总数',
               fgci_coin_num       bigint       comment '货币发放总数',
               fgbl_coin_num       bigint       comment '货币流水结余',
               fuserbl_coin_num    bigint       comment '货币用户结余'
               )comment '货币汇总结余'
               partitioned by(dt date)
        location '/dw/dcnew/currencies_balance'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fcointype','fpay_flag'],
                        'groups':[[1,1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--
        drop table if exists work.cur_balance_tmp_b_%(statdatenum)s;
        create table work.cur_balance_tmp_b_%(statdatenum)s as
                select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                       coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
                       p.fcurrencies_type fcointype,
                       case when u.fuid is not null then 1 else 0 end fpay_flag,
                       sum(p.fcurrencies_num) fact_num
                  from dim.user_currencies_balance p
                  left join dim.user_currencies_balance_day a
                    on p.fbpid = a.fbpid
                   and p.fuid = a.fuid
                   and p.fcurrencies_type = a.fcurrencies_type
                   and a.dt = "%(statdate)s"
                  left join dim.user_pay u
                    on p.fbpid = u.fbpid
                   and p.fuid = u.fuid
                  join dim.bpid_map c
                    on p.fbpid=c.fbpid
                 where p.dt='%(statdate)s'
                   and p.fcurrencies_num >= 0
                 group by c.fgamefsk,
                          c.fplatformfsk,
                          c.fhallfsk,
                          c.fterminaltypefsk,
                          c.fversionfsk,
                          c.hallmode,
                          a.fgame_id,
                          a.fchannel_code,
                          p.fcurrencies_type,
                          case when u.fuid is not null then 1 else 0 end
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
                       fcointype,
                       fpay_flag,
                       0 fgcc_coin_num,
                       0 fgci_coin_num,
                       0 fgbl_coin_num,
                       sum(fact_num) fuserbl_coin_num
                  from work.cur_balance_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fcointype,
                       fpay_flag
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        drop table if exists work.cur_balance_tmp_%(statdatenum)s;
        create table work.cur_balance_tmp_%(statdatenum)s as
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res



        hql = """
        insert overwrite table dcnew.currencies_balance
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate,
                fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fcointype,
                fpay_flag,
                sum(fgcc_coin_num) fgcc_coin_num,
                sum(fgci_coin_num) fgci_coin_num,
                sum(fgci_coin_num)-sum(fgcc_coin_num) fgbl_coin_num,
                sum(fuserbl_coin_num) fuserbl_coin_num
          from (select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fcointype,
                       fpay_flag,
                       0 fgcc_coin_num,
                       0 fgci_coin_num,
                       fuserbl_coin_num
                  from work.cur_balance_tmp_%(statdatenum)s gs
                 where fuserbl_coin_num <> 0
                union all
               select  fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk fgame_id,fterminaltypefsk,fversionfsk, fchannelcode,
                       fcointype,
                       0 fpay_flag,
                       case when fact_type = 2 then coalesce(fcoin_num,0)-coalesce(fcoin_pay_num,0) else 0 end fgcc_coin_num,
                       case when fact_type = 1 then coalesce(fcoin_num,0)-coalesce(fcoin_pay_num,0) else 0 end fgci_coin_num,
                       0 fuserbl_coin_num
                  from dcnew.currencies_detail gs
                 where dt='%(statdate)s'
                   and coalesce(fcoin_num,0) <>0
                union all
               select  fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk fgame_id,fterminaltypefsk,fversionfsk, fchannelcode,
                       fcointype,
                       1 fpay_flag,
                       case when fact_type = 2 then coalesce(fcoin_pay_num,0) else 0 end fgcc_coin_num,
                       case when fact_type = 1 then coalesce(fcoin_pay_num,0) else 0 end fgci_coin_num,
                       0 fuserbl_coin_num
                  from dcnew.currencies_detail gs
                 where dt='%(statdate)s'
                   and coalesce(fcoin_pay_num,0) <>0
               ) t
         group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                  fcointype,
                  fpay_flag
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.cur_balance_tmp_b_%(statdatenum)s;
                 drop table if exists work.cur_balance_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_currencies_balance(sys.argv[1:])
a()
