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


class agg_currencies_detail(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.currencies_detail (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fcointype           varchar(50)      comment '货币类型',
               fact_type           int              comment '操作类型：1表示+(加)，2表示-(减)',
               fact_id             varchar(50)      comment '操作编号：单个游戏内每个id表达唯一意思',
               fcoin_unum          bigint           comment '货币操作人数',
               fcoin_cnt           bigint           comment '货币操作次数',
               fcoin_num           bigint           comment '货币操作数量',
               fcoin_pay_unum      bigint           comment '货币操作人数_付费',
               fcoin_pay_cnt       bigint           comment '货币操作次数_付费',
               fcoin_pay_num       bigint           comment '货币操作数量_付费'
               )comment '货币操作表'
               partitioned by(dt date)
        location '/dw/dcnew/currencies_detail'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fcointype','fact_type','fact_id'],
                        'groups':[[1,1,1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--
        drop table if exists work.currencies_detail_tmp_b_%(statdatenum)s;
        create table work.currencies_detail_tmp_b_%(statdatenum)s as
            select '%(statdate)s' fdate,
                   c.fgamefsk,
                   c.fplatformfsk,
                   c.fhallfsk,
                   c.fterminaltypefsk,
                   c.fversionfsk,
                   c.hallmode,
                   coalesce(p.fgame_id,cast (0 as bigint)) fgame_id,
                   coalesce(cast (p.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                   fcurrencies_type fcointype,
                   p.fact_type,
                   p.fact_id,
                   case when u.fuid is not null then 1 else 0 end fpay_flag, --是否付费:0否1是
                   p.fuid,
                   abs(p.fact_num) fcoin_num
              from stage.pb_currencies_stream_stg p
              left join (select distinct fbpid, fuid
                           from dim.user_pay_day u
                          where dt = '%(statdate)s'
                        ) u
                on p.fbpid = u.fbpid
               and p.fuid = u.fuid
              join dim.bpid_map c
                on p.fbpid=c.fbpid
             where p.dt = '%(statdate)s'
               and p.fact_type in (1,2) --1:in,2:out
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
                       fact_type,
                       fact_id,
                       count(distinct fuid) fcoin_unum,
                       count(fuid) fcoin_cnt,
                       sum(fcoin_num) fcoin_num,
                       count(distinct case when fpay_flag = 1 then fuid end) fcoin_pay_unum,
                       count(case when fpay_flag = 1 then fuid end) fcoin_pay_cnt,
                       sum(case when fpay_flag = 1 then fcoin_num end) fcoin_pay_num
                  from work.currencies_detail_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fcointype,
                       fact_type,
                       fact_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.currencies_detail
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.currencies_detail_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_currencies_detail(sys.argv[1:])
a()
