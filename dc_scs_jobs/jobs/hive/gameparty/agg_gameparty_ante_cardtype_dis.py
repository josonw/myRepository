#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/service' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import sql_const

""" 统计结果在 牌局分析》底注场数据》牌型分布
    区别于 牌型概览》用户牌型(dcnew.gameparty_cardtype_dis)，防止表过大，降低查询效率所以拆出两张表 """
class agg_gameparty_ante_cardtype_dis(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_ante_cardtype_dis
        (
          fdate        date,
          fgamefsk           bigint,
          fplatformfsk       bigint,
          fhallfsk           bigint,
          fgame_id        bigint,
          fterminaltypefsk   bigint,
          fversionfsk        bigint,
          fchannel_code       bigint,
          fname        varchar(50),   --底注分布
          ftype        varchar(50),   --牌型分布
          fcnt         bigint,
          fusercnt     bigint,
          fpname       varchar(100)  --场次一级分类
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_ante_cardtype_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:  return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        extend_group = {
                     'fields':['fcard_type', 'fpname', 'fblind_1'],
                     'groups':[[1, 1, 1],
                               [1, 0, 1]
                            ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;
                              """)
        if res != 0: return res


        hql = """select
                "%(statdate)s" fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fblind_1 fname,
                fcard_type,
                count(1) fcnt,
                count(distinct(a.fuid)) fusercnt,
                coalesce(fpname, '%(null_str_group_rule)s') fpname
            from dim.user_gameparty_stream a
            where dt='%(statdate)s' and fcard_type<>'%(null_str_report)s' and hallmode = %(hallmode)s
            group by fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fgame_id,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannel_code,
                    fblind_1,
                    fcard_type,
                    fpname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """ -- 牌型场次底注分布图
        insert overwrite table dcnew.gameparty_ante_cardtype_dis
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0: return res

        return res

#生成统计实例
a = agg_gameparty_ante_cardtype_dis(sys.argv[1:])
a()
