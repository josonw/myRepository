#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_gamecoin_detail_hour_dis(BaseStatModel):
    """分时段金流
    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.gamecoin_detail_hour_dis
        (
          fdate               date,
          fgamefsk            bigint,
          fplatformfsk        bigint,
          fhallfsk            bigint,
          fsubgamefsk         bigint,
          fterminaltypefsk    bigint,
          fversionfsk         bigint,
          fchannelcode        bigint,
          fhourfsk            int           comment '游戏时段:1-24',
          fcointype           varchar(50)   comment '游戏币类型',
          fdirection          varchar(50)   comment '发放消耗',
          ftype               varchar(50)   comment '操作类型',
          funum               bigint        comment '人数',
          fact_num            bigint        comment '金币数'
        )comment '分时段金流'
     partitioned by(dt date)
        location '/dw/dcnew/gamecoin_detail_hour_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """

        extend_group = {
            'fields': ['fhourfsk', 'fdirection', 'ftype'],
            'groups': [[1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.gamecoin_detail_hour_dis_%(statdatenum)s;
        create table work.gamecoin_detail_hour_dis_%(statdatenum)s
        as
                select b.fgamefsk,
                       b.fplatformfsk,
                       b.fhallfsk,
                       b.hallmode,
                       b.fterminaltypefsk,
                       b.fversionfsk,
                       coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                       coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                       case g.act_type when 1 then 'IN' when 2 then 'OUT' end fdirection,
                       hour(lts_at)+1 fhourfsk,
                       g.act_id ftype,
                       sum(abs(act_num)) act_num ,
                       g.fuid
                  from stage.pb_gamecoins_stream_stg g
                  join dim.bpid_map b
                      on g.fbpid = b.fbpid
                where g.dt = '%(statdate)s' and g.act_type in (1, 2)
                group by  fgamefsk,
                          fplatformfsk,
                          fhallfsk,
                          hallmode,
                          fterminaltypefsk,
                          fversionfsk,
                          fgame_id,
                          fchannel_code,
                          case g.act_type when 1 then 'IN' when 2 then 'OUT' end ,
                          hour(lts_at)+1,
                          g.act_id,
                          g.fuid
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
                    fhourfsk,
                    'GAMECOIN' fcointype,
                    fdirection,
                    ftype,
                    count(distinct fuid) funum,
                    sum(act_num) fact_num
                from work.gamecoin_detail_hour_dis_%(statdatenum)s a
               where hallmode=%(hallmode)s
            group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
                     fhourfsk,
                     fdirection,
                     ftype
            """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.gamecoin_detail_hour_dis
        partition(dt='%(statdate)s')
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.gamecoin_detail_hour_dis_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_gamecoin_detail_hour_dis(sys.argv[1:])
a()
