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

class load_join_exit_room(BaseStatModel):
    def create_tab(self):
        """ 棋牌室用户状态表 """
        hql = """create table if not exists dim.join_exit_room
            (fbpid               varchar(50)     comment  'BPID',
             fgame_id            bigint          comment  '子游戏id',
             fuid                bigint          comment  '用户id',
             fjoin_dt            string          comment  '加入时间',
             fexit_dt            string          comment  '退出时间',
             fstatus             bigint          comment  '状态',
             fis_first           bigint          comment  '是否首次加入棋牌室',
             froom_id            varchar(50)     comment  '棋牌室id',
             froom_name          varchar(50)     comment  '棋牌室名称',
             fis_agent           bigint          comment  '是否代理',
             fuser_from          varchar(50)     comment  '用户来源',
             froom_partner_id    varchar(50)     comment  '棋牌室所属代理商ID',
             froom_promoter_id   varchar(50)     comment  '棋牌室所属推广员ID',
             froom_user_remark   varchar(50)     comment  '棋牌室内用户的备注',
             froom_user_job      varchar(50)     comment  '棋牌室内用户的职务'
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

        hql = """--确定当天最后一次的状态，以及各状态的时间
            drop table if exists work.join_exit_room_tmp_%(statdatenum)s;
          create table work.join_exit_room_tmp_%(statdatenum)s as
                select t.fbpid
                       ,t.fuid
                       ,t.flts_at
                       ,t.fact_id
                       ,t.fis_first
                       ,t.fgame_id
                       ,t.froom_id
                       ,t.froom_name
                       ,t.fis_agent
                       ,t.fuser_from
                       ,t.froom_partner_id
                       ,cast (t.dt as string) dt
                       ,coalesce(t.froom_promoter_id, %(null_int_report)d) froom_promoter_id
                       ,t.froom_user_remark
                       ,t.froom_user_job
                       ,row_number() over(partition by fbpid, fuid, froom_id order by flts_at desc) rownum_1
                       ,row_number() over(partition by fbpid, fuid, froom_id, fact_id order by flts_at desc) rownum_2
                  from stage.join_exit_room_stg t
                 where dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--先取当日创建，然后更新解散和禁用时间，最后确定状态
        insert overwrite table dim.join_exit_room partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.fgame_id
                       ,t1.fuid
                       ,t1.fjoin_dt
                       ,case when t2.fact_id = 2 then t2.dt else t1.fexit_dt end fexit_dt
                       ,coalesce(t2.fact_id, t1.fstatus) fstatus
                       ,case when t3.fuid is null then 1 else 0 end fis_first
                       ,t1.froom_id
                       ,t1.froom_name
                       ,t1.fis_agent
                       ,t1.fuser_from
                       ,t1.froom_partner_id
                       ,coalesce(t1.froom_promoter_id, %(null_int_report)d) froom_promoter_id
                       ,t1.froom_user_remark
                       ,t1.froom_user_job
                  from (select fbpid
                               ,fgame_id
                               ,fuid
                               ,dt fjoin_dt
                               ,dt fexit_dt
                               ,1 fstatus
                               ,fis_first
                               ,froom_id
                               ,froom_name
                               ,fis_agent
                               ,fuser_from
                               ,froom_partner_id
                               ,froom_promoter_id
                               ,froom_user_remark
                               ,froom_user_job
                          from work.join_exit_room_tmp_%(statdatenum)s t
                         where fact_id = 1
                           and rownum_2 = 1  --加入的最后一条
                         union all
                        select fbpid, fgame_id, fuid, fjoin_dt, fexit_dt, fstatus, fis_first, froom_id, froom_name, fis_agent, fuser_from, froom_partner_id, froom_promoter_id, froom_user_remark, froom_user_job
                          from dim.join_exit_room t
                         where dt = date_sub('%(statdate)s' ,1)
                       ) t1
                  left join work.join_exit_room_tmp_%(statdatenum)s t2
                    on t1.fbpid = t2.fbpid
                   and t1.fuid = t2.fuid
                   and t1.froom_id = t2.froom_id
                   and t2.rownum_1 = 1  --当天的最后一条确定状态
                  left join (select distinct froom_id,fbpid,fuid from dim.join_exit_room t where dt = date_sub('%(statdate)s',1)) t3
                    on t1.fbpid = t3.fbpid
                   and t1.fuid = t3.fuid
                   and t1.froom_id = t3.froom_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.join_exit_room_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_join_exit_room(sys.argv[1:])
a()
