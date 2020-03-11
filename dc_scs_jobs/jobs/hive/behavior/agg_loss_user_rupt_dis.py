# -*- coding: UTF-8 -*-
#src     :dim.user_reflux,stage.user_bankrupt_stg,dim.bpid_map
#dst     :dcnew.loss_user_rupt_dis
#authot  :SimonRen
#date    :2016-09-02


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


class agg_loss_user_rupt_dis(BaseStatModel):
    def create_tab(self):

        hql = """--流失用户破产次数分布
        create table if not exists dcnew.loss_user_rupt_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               frupt_cnt           int        comment '破产次数',
               flossdays           int        comment '流失天数',
               fcloss_unum         bigint     comment 'cycle回流用户数',
               fdloss_unum         bigint     comment 'day流失回流用户数'
               )comment '流失用户_破产次数'
               partitioned by(dt date)
        location '/dw/dcnew/loss_user_rupt_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['flossdays','frupt_cnt'],
                        'groups':[[1, 1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """
        drop table if exists work.loss_user_rupt_dis_tmp_b_%(statdatenum)s;
        create table work.loss_user_rupt_dis_tmp_b_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               c.hallmode,
                               a.fgame_id,
                               a.fchannel_code,
                               a.fuid,
                               a.days flossdays,
                               a.ruptcnt frupt_cnt
                          from dim.user_churn a
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
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
                       frupt_cnt,
                       flossdays,
                       0 fcloss_unum,
                       count(distinct fuid) fdloss_unum
                  from work.loss_user_rupt_dis_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       flossdays,
                       frupt_cnt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.loss_user_rupt_dis
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.loss_user_rupt_dis_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_loss_user_rupt_dis(sys.argv[1:])
a()
