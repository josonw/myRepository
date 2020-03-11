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


class agg_gameparty_ante_property_dis(BaseStatModel):
    """牌局底注，流通分布数据

    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_ante_property_dis
        (
          fdate              date,
          fgamefsk           bigint,
          fplatformfsk       bigint,
          fhallfsk           bigint,
          fgame_id        bigint,
          fterminaltypefsk   bigint,
          fversionfsk        bigint,
          fchannel_code       bigint,
          fpname             varchar(100),
          fante              bigint,
          fname              varchar(50),
          fpartynum          bigint
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_ante_property_dis'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        extend_group = {
                     'fields':['fpname', 'fante', 'fname'],
                     'groups':[[1, 1, 1],
                               [0, 1, 1]  ] }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """ -- 牌局流通
            select "%(statdate)s" fdate,
                    fgamefsk,
                    coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                    coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                    coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                    coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                    coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                    coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                    coalesce(fpname, '%(null_str_group_rule)s') fpname,
                    fante,
                    fname,
                    count(1) fpartynum
            from (
                select
                    fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk,
                    ftbl_id, finning_id,
                    coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                    coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
                    coalesce(a.fpname,'%(null_str_report)s') fpname,
                    coalesce(a.fante,'%(null_str_report)s') fante,
                    hallmode,
                    case
                        when fwin_amt+flose_amt <= 0 then  '0' --存string
                        when fwin_amt+flose_amt >= 1 and fwin_amt+flose_amt < 5000 then '1-5000'
                        when fwin_amt+flose_amt >= 5000 and fwin_amt+flose_amt < 10000 then '5000-1万'
                        when fwin_amt+flose_amt >= 10000 and fwin_amt+flose_amt < 50000 then  '1万-5万'
                        when fwin_amt+flose_amt >= 50000 and fwin_amt+flose_amt < 100000 then '5万-10万'
                        when fwin_amt+flose_amt >= 100000 and fwin_amt+flose_amt < 500000 then '10万-50万'
                        when fwin_amt+flose_amt >= 500000 and fwin_amt+flose_amt < 1000000 then '50万-100万'
                        when fwin_amt+flose_amt >= 1000000 and fwin_amt+flose_amt < 5000000 then '100万-500万'
                        when fwin_amt+flose_amt >= 5000000 and fwin_amt+flose_amt < 10000000 then '500万-1000万'
                        when fwin_amt+flose_amt >= 10000000 and fwin_amt+flose_amt < 50000000 then '1000万-5000万'
                        when fwin_amt+flose_amt >= 50000000 and fwin_amt+flose_amt < 100000000 then '5000万-1亿'
                        when fwin_amt+flose_amt >= 100000000 and fwin_amt+flose_amt < 1000000000 then '1亿-10亿'
                    else '10亿+' end fname
                from dim.gameparty_stream a
                where a.dt = "%(statdate)s" and hallmode = %(hallmode)s
            ) a
           group by fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fgame_id,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannel_code,
                    fpname,
                    fante,
                    fname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.gameparty_ante_property_dis
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


#生成统计实例
a = agg_gameparty_ante_property_dis(sys.argv[1:])
a()
