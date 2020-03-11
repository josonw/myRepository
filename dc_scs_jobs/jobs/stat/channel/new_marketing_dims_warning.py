#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class new_marketing_dims_warning(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.marketing_channel_dims_warning
        (
          fdate       date,
          fchannel_id varchar(64),
          fdim_name   varchar(64),
          fdim_7avg   decimal(20,2),
          fdim_value  decimal(20,2),
          frate       decimal(20,2)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []

        hql = """
        insert overwrite table analysis.marketing_channel_dims_warning partition(dt = "%(ld_begin)s")
        select fdate,
             fchannel_id,
             'dsu' fdim_name,
             fdsu_d_avg fdim_7avg,
             fdsu_d fdim_value,
             round(case
                     when fdsu_d_avg = 0 then
                      0
                     else
                      (fdsu_d - fdsu_d_avg) / fdsu_d_avg * 100
                   end,
                   2) frate
        from (select '%(ld_begin)s' fdate,
                     fchannel_id,
                     nvl(round(avg(case
                                     when fdate < '%(ld_begin)s' then
                                      fdsu_d
                                   end),
                               2),
                         0) fdsu_d_avg,
                     nvl(sum(case
                               when fdate = '%(ld_begin)s' then
                                fdsu_d
                             end),
                         0) fdsu_d
                from analysis.marketing_channel_dims_fct
               where dt >= '%(ld_7dayago)s'
                 and dt < '%(ld_end)s'
               group by fchannel_id ) as abc
       where fdsu_d_avg >= 50
       union all
       select fdate,
              fchannel_id,
              'dip' fdim_name,
              fdip_avg fdim_7avg,
              fdip fdim_value,
              round(case
                      when fdip_avg = 0 then
                       0
                      else
                       (fdip - fdip_avg) / fdip_avg * 100
                    end,
                    2) frate
         from (select '%(ld_begin)s' fdate,
                      fchannel_id,
                      nvl(round(avg(case
                                      when fdate < '%(ld_begin)s' then
                                       fdip
                                    end),
                                2),
                          0) fdip_avg,
                      nvl(sum(case
                                when fdate = '%(ld_begin)s' then
                                 fdip
                              end),
                          0) fdip
                 from analysis.marketing_channel_dims_fct
                where dt >= '%(ld_7dayago)s'
                  and dt < '%(ld_end)s'
                group by fchannel_id) as abc
        where fdip_avg >= 50
        """ % self.hql_dict
        hql_list.append( hql )


        result = self.exe_hql_list(hql_list)
        return result


if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = new_marketing_dims_warning(stat_date)
    a()
