#-*- coding: UTF-8 -*-
# Author:AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_user_gamecoins_all(BaseStat):
    def create_tab(self):
        """
        用户金币变动全量表
        ffirst_act_date：    首次变动日期
        flast_act_date：     最后一次变动日期
        fgamecoins_num：     当前金币数
        """
        hql = """
          create table if not exists stage.user_gamecoins_all
          (
            fdate           date,
            fbpid           varchar(64),
            fuid            int,
            ffirst_act_date date,
            flast_act_date  date,
            fgamecoins_num bigint
          )
          partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """drop table if exists stage.user_gamecoins_all_tmp_%(num_begin)s;
                  create table stage.user_gamecoins_all_tmp_%(num_begin)s as
                  select fbpid,
                         fuid,
                         ffirst_act_date,
                         flast_act_date,
                         fgamecoins_num
                    from stage.user_gamecoins_all
                   where dt = '%(ld_1dayago)s'

                   union all

                  select fbpid,
                         fuid,
                         to_date(fdate) ffirst_act_date,
                         to_date(fdate) flast_act_date,
                         user_gamecoins_num fgamecoins_num
                    from stage.pb_gamecoins_stream_mid
                    where dt = '%(ld_daybegin)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """insert overwrite table stage.user_gamecoins_all partition
                  (dt = '%(ld_daybegin)s')
                  select '%(ld_daybegin)s' fdate,
                         fbpid,
                         fuid,
                         min(ffirst_act_date) ffirst_act_date,
                         max(flast_act_date) flast_act_date,
                         max(fgamecoins_num) fgamecoins_num
                    from (select fbpid,
                                 fuid,
                                 min(ffirst_act_date) ffirst_act_date,
                                 null flast_act_date,
                                 null fgamecoins_num
                            from stage.user_gamecoins_all_tmp_%(num_begin)s
                           group by fbpid, fuid

                          union all

                          select fbpid,
                                 fuid,
                                 ffirst_act_date,
                                 flast_act_date,
                                 fgamecoins_num
                            from (select fbpid,
                                         fuid,
                                         null ffirst_act_date,
                                         flast_act_date,
                                         fgamecoins_num,
                                         row_number() over(partition by fbpid, fuid order by flast_act_date desc, fgamecoins_num desc) rown
                                    from stage.user_gamecoins_all_tmp_%(num_begin)s) t1
                           where rown = 1) t2
                   group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """drop table if exists stage.user_gamecoins_all_tmp_%(num_begin)s """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  global statDate
  if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
  else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
  #生成统计实例
  a = agg_user_gamecoins_all(statDate)
  a()
