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


class agg_gameparty_hour_dis(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_hour_dis
        (
          fdate           date,
          fgamefsk           bigint,
          fplatformfsk       bigint,
          fhallfsk           bigint,
          fsubgamefsk        bigint,
          fterminaltypefsk   bigint,
          fversionfsk        bigint,
          fchannelcode       bigint,
          fpname       varchar(100),
          fblind_1        bigint,
          fhourfsk        bigint,
          fusernum     bigint,
          fplayusernum bigint,
          fpartynum    bigint,
          fcharge      decimal(30,2)
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_hour_dis'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容  """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.hq.debug = 0
        extend_group = {
                     'fields': ['fpname', 'fblind_1', 'hour(flts_at)+1'],
                     'groups': [[1, 1, 1],
                                [0, 1, 1],
                                [0, 0, 1]]}

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        select
            '%(statdate)s' fdate,
            fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
            coalesce(fpname, '%(null_str_group_rule)s') fpname,
            fblind_1,
            hour(flts_at)+1 fhourfsk,
            count(1) fusernum,
            count(distinct fuid) fplayusernum,
            count(distinct concat_ws('0', ftbl_id, finning_id) ) fpartynum,
            sum(fcharge) fcharge
        from dim.user_gameparty_stream
        where hallmode = %(hallmode)s
        and dt='%(statdate)s'
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fpname,
                fblind_1,
                hour(flts_at)+1
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.gameparty_hour_dis
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


#生成统计实例
a = agg_gameparty_hour_dis(sys.argv[1:])
a()
