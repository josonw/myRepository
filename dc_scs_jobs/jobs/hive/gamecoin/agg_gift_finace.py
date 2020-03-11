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


class agg_gift_finace(BaseStatModel):
    def create_tab(self):

        hql = """
        --礼物分析，包括购买礼物的币种、次数、数量、人数等
        create table if not exists dcnew.gift_finace (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fgift_id            varchar(50)    comment    '礼物ID',
               fc_type             int            comment    '购买礼物的币种:1博雅币,2游戏币,0其他',
               fdirection          varchar(50)    comment    '操作类型:IN\OUT',
               fgift_cnt           bigint         comment    '购买礼物的次数',
               fgift_num           bigint         comment    '购买礼物的数量',
               fgift_unum          bigint         comment    '购买礼物的人数'
               )comment '礼物数据表'
               partitioned by(dt date)
        location '/dw/dcnew/gift_finace';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fgift_id','fc_type','fdirection'],
                        'groups':[[1, 1, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--取购买礼物的币种、次数、数量、人数
        drop table if exists work.gift_finace_tmp_b_%(statdatenum)s;
        create table work.gift_finace_tmp_b_%(statdatenum)s as
                select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       coalesce(p.fgame_id,cast (0 as bigint)) fgame_id,
                       coalesce(cast (p.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                       p.gift_id fgift_id,
                       p.c_type fc_type,
                       case when p.act_type = 1 then 'IN'
                            when p.act_type = 2 then 'OUT' end fdirection,
                       sum(abs(p.act_num)) fgift_num,
                       p.fuid,
                       count(1) cnt
                  from stage.pb_gifts_stream_stg p
                  join dim.bpid_map c
                    on p.fbpid=c.fbpid
                 where p.dt = "%(statdate)s"
                 group by c.fgamefsk,
                          c.fplatformfsk,
                          c.fhallfsk,
                          c.fterminaltypefsk,
                          c.fversionfsk,
                          c.hallmode,
                          p.fgame_id,
                          p.fchannel_code,
                          p.gift_id,
                          p.c_type,
                          case when p.act_type = 1 then 'IN'
                               when p.act_type = 2 then 'OUT' end,
                          p.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据并导入到正式表上
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fgift_id,
                       fc_type,
                       fdirection,
                       sum(cnt) fgift_cnt, --操作礼物的次数
                       sum(fgift_num) fgift_num, --操作礼物的数量
                       count(distinct fuid) fgift_unum --操作礼物的用户数
                  from work.gift_finace_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fgift_id,
                       fc_type,
                       fdirection
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert overwrite table dcnew.gift_finace
        partition (dt = '%(statdate)s')
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.gift_finace_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_gift_finace(sys.argv[1:])
a()
