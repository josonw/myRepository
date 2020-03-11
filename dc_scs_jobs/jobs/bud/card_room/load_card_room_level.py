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

class load_card_room_level(BaseStatModel):
    def create_tab(self):
        """ 棋牌室代理商等级及房卡结余中间表 """
        hql = """create table if not exists dim.card_room_level
            (fdate           string           comment  '最后一次变动时间',
             fbpid           varchar(50)    comment  'BPID',
             fpartner_uid    bigint         comment  '代理商UID',
             fpromoter       varchar(50)    comment  '推广员',
             fcard_room_id   varchar(50)    comment  '棋牌室id',
             fgame_level     bigint         comment  '等级'
            )
            partitioned by (dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分,统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.card_room_level partition (dt = "%(statdate)s" )
        select fdate,fbpid,fpartner_uid,coalesce(fpromoter, '%(null_str_report)s') fpromoter,fcard_room_id,coalesce(fgame_level, %(null_int_report)d) fgame_level
          from (select fdate,fbpid,fpartner_uid,fpromoter,fcard_room_id,fgame_level,
                       row_number() over(partition by fbpid, fpartner_uid order by fdate desc) rownum
                  from (select '%(statdate)s' fdate
                               ,t1.fbpid
                               ,t1.fpartner_info fpartner_uid
                               ,t2.fpromoter
                               ,fcard_room_id
                               ,max(fgame_level) fgame_level
                          from (select fbpid,cast (t1.fpartner_info as bigint) fpartner_info, fgame_id,fgame_level,fleft_num,fgsubname fcard_room_id,
                                       row_number() over(partition by fbpid, fpartner_info, fgame_id ,fgsubname order by flts_at desc,fgame_level desc,fleft_num) rownum
                                  from stage.open_room_record_stg t1
                                 where dt = '%(statdate)s'
                               ) t1
                          left join dim.card_room_partner_new t2
                            on t1.fbpid = t2.fbpid
                         where t1.rownum = 1
                         group by t1.fbpid
                                  ,t1.fpartner_info
                                  ,t2.fpromoter
                                  ,fcard_room_id

                         union all

                        select fdate
                               ,fbpid
                               ,fpartner_uid
                               ,fpromoter
                               ,fcard_room_id
                               ,fgame_level
                          from dim.card_room_level
                         where dt = date_sub('%(statdate)s',1)
                        ) t
               ) t
         where rownum = 1

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_card_room_level(sys.argv[1:])
a()
