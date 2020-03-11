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



class agg_pay_user_continue_pay_days(BaseStatModel):
    """ 截止到统计日期，付费用户的平均连续付费天数， 最大的连续付费天数(从统计日期开始往前推)，
        多个时间段内的最大最小付费次数"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_continue_pay_days
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            favgconpayday               bigint,
            fmaxconpayday               bigint,
            f1daymaxpaycnt              bigint,
            f1dayminpaycnt              bigint,
            f3daymaxpaycnt              bigint,
            f3dayminpaycnt              bigint,
            f7daymaxpaycnt              bigint,
            f7dayminpaycnt              bigint,
            f30daymaxpaycnt             bigint,
            f30dayminpaycnt             bigint
        )
        partitioned by (dt date);
        """
        result = self.sql_exe(hql)
        if result != 0:return result


    def stat(self):
        """ 重要部分，统计内容 """

        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}

        query = sql_const.query_list(self.stat_date, alias_dic, None)

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_continue_pay_days1_%(statdatenum)s;
        create table if not exists work.pay_user_continue_pay_days1_%(statdatenum)s as
        select fbpid,
            fuid,
            fgame_id,
            fchannel_code,
            dt,
            count(1) over(partition by fbpid, fuid,fgame_id,fchannel_code, fflag) fconpayday,
            fpay_cnt
        from
        (
            select fbpid,
                fuid,
                fgame_id,
                fchannel_code,
                dt,
                date_add(dt, -dense_rank() over(partition by fbpid, fuid,fgame_id,fchannel_code order by dt) ) fflag,
                fpay_cnt
            from dim.user_pay_day
           where dt < '%(ld_end)s'
        ) a
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_continue_pay_days2_%(statdatenum)s;
        create table if not exists work.pay_user_continue_pay_days2_%(statdatenum)s as
        select fbpid,
              fuid,
              fgame_id,
              fchannel_code,
              max(case when ud.dt >= '%(statdate)s' and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f1daymaxpaycnt,
              min(case
                    when ud.dt >= '%(statdate)s' and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f1dayminpaycnt,
              max(case
                    when ud.dt >= date_add('%(ld_end)s',-3) and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f3daymaxpaycnt,
              min(case
                    when ud.dt >= date_add('%(ld_end)s',-3) and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f3dayminpaycnt,
              max(case
                    when ud.dt >= date_add('%(ld_end)s',-7) and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f7daymaxpaycnt,
               min(case
                    when ud.dt >= date_add('%(ld_end)s',-7) and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f7dayminpaycnt,
              max(case
                    when ud.dt >= date_add('%(ld_end)s',-30) and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f30daymaxpaycnt,
              min(case
                    when ud.dt >= date_add('%(ld_end)s',-30) and
                         ud.dt < '%(ld_end)s' then
                     ud.fpay_cnt
                    else
                     null
                  end) f30dayminpaycnt
         from dim.user_pay_day ud
        where dt >= date_add('%(ld_end)s',-30) and dt < '%(ld_end)s'
        group by fbpid, fuid, fgame_id, fchannel_code
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        for k, v in enumerate(query):
            hql = """
                select '%(statdate)s' fdate,
                        %(select_field_str)s,
                        floor(avg(a.fconpayday)) favgconpayday,
                        max(a.fconpayday) fmaxconpayday,
                        0 f1daymaxpaycnt,
                        0 f1dayminpaycnt,
                        0 f3daymaxpaycnt,
                        0 f3dayminpaycnt,
                        0 f7daymaxpaycnt,
                        0 f7dayminpaycnt,
                        0 f30daymaxpaycnt,
                        0 f30dayminpaycnt
                     from work.pay_user_continue_pay_days1_%(statdatenum)s a
                     join dim.bpid_map b
                       on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                     where a.dt >= '%(statdate)s'
                       and a.dt < '%(ld_end)s'
                    %(group_by)s
            """
            self.sql_args['sql_'+ str(k)] = self.sql_build(hql, v)


        hql = """
        drop table if exists work.pay_user_continue_pay_days3_%(statdatenum)s;
        create table work.pay_user_continue_pay_days3_%(statdatenum)s as
        %(sql_0)s;
        insert into table work.pay_user_continue_pay_days3_%(statdatenum)s
        %(sql_1)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        for k, v in enumerate(query):
            hql = """
            insert into table work.pay_user_continue_pay_days3_%(statdatenum)s
                select '%(statdate)s' fdate,
                      %(select_field_str)s,
                      0 favgconpayday,
                      0 fmaxconpayday,
                      nvl(max(a.f1daymaxpaycnt), 0) f1daymaxpaycnt,
                      nvl(min(case a.f1dayminpaycnt when 0 then null else a.f1dayminpaycnt end), 0) f1dayminpaycnt,
                      nvl(max(a.f3daymaxpaycnt), 0) f3daymaxpaycnt,
                      nvl(min(case a.f3dayminpaycnt when 0 then null else a.f3dayminpaycnt end), 0) f3dayminpaycnt,
                      nvl(max(a.f7daymaxpaycnt), 0) f7daymaxpaycnt,
                      nvl(min(case a.f7dayminpaycnt when 0 then null else a.f7dayminpaycnt end), 0) f7dayminpaycnt,
                      nvl(max(a.f30daymaxpaycnt), 0) f30daymaxpaycnt,
                      nvl(min(case a.f30dayminpaycnt when 0 then null else a.f30dayminpaycnt end), 0) f30dayminpaycnt
                 from work.pay_user_continue_pay_days2_%(statdatenum)s a
                 join dim.bpid_map b
                   on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                %(group_by)s
            """
            hql = self.sql_build(hql, v)
            res = self.sql_exe(hql)
            if res != 0:return res


        hql = """
        insert overwrite table dcnew.pay_user_continue_pay_days
        partition( dt="%(statdate)s" )
        select
            fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
            max(favgconpayday)          favgconpayday,
            max(fmaxconpayday)          fmaxconpayday,
            max(f1daymaxpaycnt)         f1daymaxpaycnt,
            max(f1dayminpaycnt)         f1dayminpaycnt,
            max(f3daymaxpaycnt)         f3daymaxpaycnt,
            max(f3dayminpaycnt)         f3dayminpaycnt,
            max(f7daymaxpaycnt)         f7daymaxpaycnt,
            max(f7dayminpaycnt)         f7dayminpaycnt,
            max(f30daymaxpaycnt)        f30daymaxpaycnt,
            max(f30dayminpaycnt)        f30dayminpaycnt
        from work.pay_user_continue_pay_days3_%(statdatenum)s
        group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        sql = """
        drop table if exists work.pay_user_continue_pay_days1_%(statdatenum)s;
        drop table if exists work.pay_user_continue_pay_days2_%(statdatenum)s;
        drop table if exists work.pay_user_continue_pay_days3_%(statdatenum)s;
        """
        res = self.sql_exe(sql)
        return res


#生成统计实例
a = agg_pay_user_continue_pay_days(sys.argv[1:])
a()
