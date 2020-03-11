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


class agg_xxx_user_loss_reflux_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.xxx_user_loss_reflux_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fuser_type          varchar(10)   comment '用户类型：ad等',
               fuser_type_id       varchar(100)  comment '用户类型id',
               freflux             int           comment '流失天数',
               floss_unum          bigint        comment '对应单日回流用户人数',
               freflux_unum        bigint        comment '对应回流用户人数'
               )comment 'xxx用户流失回流表'
               partitioned by(dt date)
        location '/dw/dcnew/xxx_user_loss_reflux_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_type_id', 'fuid'],
                        'groups': [[1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--
                  drop table if exists work.xxx_user_loss_reflux_info_tmp_a_%(statdatenum)s;
                create table work.xxx_user_loss_reflux_info_tmp_a_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,coalesce(t3.fuser_type,'ad') fuser_type
                 ,coalesce(t3.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.fuid
            from dim.user_act t1
            left join dim.xxx_user t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt <= '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
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
                       fuser_type,
                       fuser_type_id,
                       fuid,
                       count(1) num
                  from work.xxx_user_loss_reflux_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       fuid
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """drop table if exists work.xxx_user_loss_reflux_info_tmp_%(statdatenum)s;
          create table work.xxx_user_loss_reflux_info_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 插入目标表
        hql = """  insert overwrite table dcnew.xxx_user_loss_reflux_info partition(dt='%(statdate)s')
                   select "%(statdate)s" fdate
                          ,a.fgamefsk
                          ,a.fplatformfsk
                          ,a.fhallfsk
                          ,a.fgame_id
                          ,a.fterminaltypefsk
                          ,a.fversionfsk
                          ,a.fchannel_code
                          ,a.fuser_type
                          ,a.fuser_type_id
                          ,b.freflux       --流失天数
                          ,count(distinct case when b.freflux_type='day' then b.fuid end) floss_unum    --对应单日回流用户人数
                          ,count(distinct case when b.freflux_type='cycle' then b.fuid end) freflux_unum  --对应回流用户人数
                     from work.xxx_user_loss_reflux_info_tmp_%(statdatenum)s a
                     join dim.user_reflux_array b
                       on a.fgamefsk = b.fgamefsk
                      and a.fplatformfsk = b.fplatformfsk
                      and a.fhallfsk = b.fhallfsk
                      and a.fgame_id = b.fgame_id
                      and a.fterminaltypefsk = b.fterminaltypefsk
                      and a.fversionfsk = b.fversionfsk
                      and a.fchannel_code = b.fchannel_code
                      and a.fuid = b.fuid
                      and b.dt = '%(statdate)s'
                    group by a.fgamefsk,a.fplatformfsk,a.fhallfsk,a.fgame_id,a.fterminaltypefsk,a.fversionfsk,a.fchannel_code,
                             a.fuser_type,a.fuser_type_id,b.freflux
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.xxx_user_loss_reflux_info_tmp_a_%(statdatenum)s;
                 drop table if exists work.xxx_user_loss_reflux_info_tmp_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_xxx_user_loss_reflux_info(sys.argv[1:])
a()
