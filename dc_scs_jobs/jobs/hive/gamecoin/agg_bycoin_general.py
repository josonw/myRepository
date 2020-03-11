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


class agg_bycoin_general(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.bycoin_general (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fcointype           varchar(50)      comment '游戏币类型:bycoin',
               fdirection          varchar(10)      comment '操作类型:IN\OUT',
               ftype               varchar(50)      comment '操作编号',
               fpay_flag           int              comment '用户是否付费:0否1是',
               fby_unum            bigint           comment '博雅币操作人数',
               fby_cnt             bigint           comment '博雅币操作次数',
               fby_num             bigint           comment '博雅币操作数量'
               )comment '博雅币操作表'
               partitioned by(dt date)
        location '/dw/dcnew/bycoin_general'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fcointype','fdirection','ftype','fpay_flag'],
                        'groups':[[0,1,1,1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--
        drop table if exists work.bycoin_general_tmp_b_%(statdatenum)s;
        create table work.bycoin_general_tmp_b_%(statdatenum)s as
            select '%(statdate)s' fdate,
                   c.fgamefsk,
                   c.fplatformfsk,
                   c.fhallfsk,
                   c.fterminaltypefsk,
                   c.fversionfsk,
                   c.hallmode,
                   coalesce(p.fgame_id,cast (0 as bigint)) fgame_id,
                   coalesce(cast (p.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                   'BYCOIN' fcointype,
                   case when p.fact_type=1 then 'IN'
                        when p.fact_type=2 then 'OUT'
                   end fdirection,
                   p.fact_id ftype,
                   case when u.fuid is not null then 1 else 0 end fpay_flag, --是否付费:0否1是
                   p.fuid,
                   abs(p.fact_num) fby_num
              from stage.pb_bycoins_stream_stg p
              left join dim.user_pay_day u
                on p.fbpid = u.fbpid
               and p.fuid = u.fuid
               and u.dt = '%(statdate)s'
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
                       fdirection,
                       ftype,
                       fpay_flag,
                       count(distinct fuid) fby_unum,
                       count(fuid) fby_cnt,
                       sum(fby_num) fby_num
                  from work.bycoin_general_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fcointype,
                       fdirection,
                       ftype,
                       fpay_flag
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.bycoin_general
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bycoin_general_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_bycoin_general(sys.argv[1:])
a()
