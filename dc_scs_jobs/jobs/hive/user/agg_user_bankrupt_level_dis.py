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


class agg_user_bankrupt_level_dis(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_bankrupt_level_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               flevel              int           comment '用户等级',
               fbankrupt_unum      bigint        comment '破产人数',
               fbankrupt_cnt       bigint        comment '破产次数',
               frelief_unum        bigint        comment '救济人数',
               frelief_cnt         bigint        comment '救济次数'
               )comment '破产用户等级分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_bankrupt_level_dis';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['flevel'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """--取出破产救济用户及次数
        drop table if exists work.user_brupt_level_tmp_1_%(statdatenum)s;
        create table work.user_brupt_level_tmp_1_%(statdatenum)s as
              select fbpid,
                     fuid,
                     max(flevel) flevel
                from (
                      -- 一个用户一天里等级有很多级，有很多垃圾数据，所以取最后一条
                      -- 等级大于300的当垃圾处理
                      select fbpid, fuid, nvl(flevel,0) flevel
                        from (select fbpid,
                                     fuid,
                                     flevel,
                                     row_number() over(partition by fbpid, fuid order by fgrade_at desc, flevel desc) as rowcn
                                from stage.user_grade_stg
                               where dt = '%(statdate)s'
                                 and flevel < 300
                             ) a
                       where rowcn = 1
                       union all
                          -- 还要从登陆的级别里更新，最后根据升级和登陆取最大值
                      select fbpid, fuid,nvl(flevel,0) flevel
                        from (select fbpid,
                                     fuid,
                                     flevel,
                                     row_number() over(partition by fbpid,fuid order by flogin_at desc ) rowcn
                                from dim.user_login_additional
                               where dt='%(statdate)s'
                                 and flevel < 300 and nvl(flevel,0) > 0
                             ) a
                       where rowcn = 1
                    ) t1
                    join dim.bpid_map tt
                      on t1.fbpid = tt.fbpid
                    group by t1.fbpid, fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.user_brupt_level_tmp_2_%(statdatenum)s;
        create table work.user_brupt_level_tmp_2_%(statdatenum)s  as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t2.flevel, 0) flevel
                 ,t1.fuid
                 ,t1.frupt_cnt
                 ,t1.frlv_cnt
            from dim.user_bankrupt_relieve t1
            left join work.user_brupt_level_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt='%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合破产救济用户及次数数据
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,%(null_int_group_rule)d fchannelcode
                 ,flevel
                 ,count(distinct case when frupt_cnt>0 then fuid end) fbankrupt_unum
                 ,sum(frupt_cnt) fbankrupt_cnt
                 ,count(distinct case when frlv_cnt>0 then fuid end) frelief_unum
                 ,sum(frlv_cnt) frelief_cnt
            from work.user_brupt_level_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,flevel
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """insert overwrite table dcnew.user_bankrupt_level_dis
                partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_brupt_level_tmp_1_%(statdatenum)s;
                 drop table if exists work.user_brupt_level_tmp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_bankrupt_level_dis(sys.argv[1:])
a()
