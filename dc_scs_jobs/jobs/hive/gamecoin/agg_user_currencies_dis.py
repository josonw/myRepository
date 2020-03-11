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


class agg_user_currencies_dis(BaseStatModel):
    def create_tab(self):

        hql = """
        --货币分布,用户当日初始货币携带量
        create table if not exists dcnew.user_currencies_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fc_type             varchar(50)   comment '货币类型',
               funum               bigint        comment '用户数',
               flft                bigint        comment '区间下界',
               frgt                bigint        comment '区间上界'
               )comment '货币分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_currencies_dis';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fc_type','flft','frgt'],
                        'groups':[[1, 1, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--取用户当日初始货币携带量
        drop table if exists work.currencies_dis_tmp_b_%(statdatenum)s;
        create table work.currencies_dis_tmp_b_%(statdatenum)s as
              select c.fgamefsk,
                     c.fplatformfsk,
                     c.fhallfsk,
                     c.fterminaltypefsk,
                     c.fversionfsk,
                     c.hallmode,
                     coalesce(t.fgame_id,cast (0 as bigint)) fgame_id,
                     coalesce(t.fchannel_code,%(null_int_report)d) fchannel_code,
                     t.fuid,
                     t.fcurrencies_type,
                     t.fcurrencies_num
                from  ( select fbpid,
                               fuid,
                               fgame_id,
                               fchannel_code,
                               fcurrencies_type,
                               fcurrencies_num
                          from (select fbpid,
                                       fuid,
                                       fgame_id,
                                       cast (fchannel_code as bigint)  fchannel_code,
                                       fcurrencies_type,
                                       fcurrencies_num,
                                       row_number() over(partition by fbpid, fuid, fcurrencies_type order by flts_at,fseq_no) rown
                                  from stage.pb_currencies_stream_stg
                                 where dt = "%(statdate)s"
                               ) ss
                         where ss.rown = 1
                      ) t
                join dim.bpid_map c
                  on t.fbpid=c.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--将用户当日初始货币携带量，转换为区间数据
        drop table if exists work.currencies_dis_tmp_b_2_%(statdatenum)s;
        create table work.currencies_dis_tmp_b_2_%(statdatenum)s as
              select a.fgamefsk,
                     a.fplatformfsk,
                     a.fhallfsk,
                     a.fterminaltypefsk,
                     a.fversionfsk,
                     a.hallmode,
                     a.fgame_id,
                     a.fchannel_code,
                     a.fcurrencies_type fc_type,
                     a.fuid,
                     b.flft,
                     b.frgt
                from work.currencies_dis_tmp_b_%(statdatenum)s a
                join stage.jw_qujian b
               where a.fcurrencies_num >= b.flft
                 and a.fcurrencies_num < b.frgt;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据 并导入到正式表上
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fc_type,
                       count(distinct gs.fuid) fcnt,
                       flft,
                       frgt
                  from work.currencies_dis_tmp_b_2_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fc_type,
                       flft,
                       frgt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert overwrite table dcnew.user_currencies_dis
        partition (dt = '%(statdate)s')
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.currencies_dis_tmp_b_%(statdatenum)s;
                 drop table if exists work.currencies_dis_tmp_b_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_currencies_dis(sys.argv[1:])
a()
