#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class audit_gamecoin_bycoin(BaseStat):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        use stage;
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": self.stat_date,
            "ld_end": PublicFunc.add_days(self.stat_date, 1),
            "num_date": self.stat_date.replace("-", ""),
            "del_num_date": PublicFunc.add_days(self.stat_date, -7).replace('-', '')
        }


##########################################################################
# 游戏币审计
##########################################################################
        hql = """
            drop table if exists stage.audit_gamecoin_error_record_tmp_%(num_date)s;
            
            create table stage.audit_gamecoin_error_record_tmp_%(num_date)s as
            select fgamefsk,
                   fplatformfsk,
                   fbpid,
                   fuid,
                   flts_at,
                   fact_type,
                   fact_id,
                   fact_num,
                   fuser_gamecoins_num,
                   fseq_no,
                   fdiff,
                   '%(ld_begin)s' as faudit_date
              from (select fgamefsk,
                           fplatformfsk,
                           fbpid,
                           fuid,
                           flts_at,
                           fact_type,
                           fact_id,
                           fact_num,
                           fuser_gamecoins_num,
                           fseq_no fseq_no,
                           fdiff,
                           sum(fdiff) over(partition by fgamefsk, fplatformfsk, fuid order by flts_at, fseq_no rows between unbounded preceding and unbounded following) as fdiff_sum
                      from (select b.fgamefsk,
                                   b.fplatformfsk,
                                   a.fbpid,
                                   a.fuid,
                                   a.lts_at as flts_at,
                                   a.act_type as fact_type,
                                   a.act_id as fact_id,
                                   a.act_num as fact_num,
                                   a.user_gamecoins_num as fuser_gamecoins_num,
                                   a.fseq_no,
                                   nvl(a.user_gamecoins_num - if(a.act_type = 1, 1, -1) * abs(a.act_num) - lag(a.user_gamecoins_num) over(partition by b.fgamefsk,b.fplatformfsk,a.fuid order by a.lts_at,a.fseq_no), 0) as fdiff
                              from stage.pb_gamecoins_stream_stg a
                              join dim.bpid_map b
                                on a.fbpid = b.fbpid
                             where b.fgamefsk not in (4389900412,
                                                      4118194431,
                                                      33658545,
                                                      1396899,
                                                      54871823,
                                                      4204924431,
                                                      60321414,
                                                      60321415,
                                                      60321416,
                                                      60321417,
                                                      60321418,
                                                      1731825777,
                                                      1752221033,
                                                      1767597564)
                               and a.fbpid not in
                                   ('A3277E8003DD1C1E40FA863EC04EB68A',
                                    'CC2C1C913F7B18AFFBBAF731ABCAEA34',
                                    '6481B0B56DFE0CE0E7A701E54F648E97')
                               and a.dt >= '%(ld_begin)s'
                               and a.dt < '%(ld_end)s'
                               and a.act_type in (1, 2)) t1) t2
             where fdiff_sum != 0;
        """ % args_dic

        hql += """
      insert overwrite table analysis.audit_gamecoin_error_sample
      partition(dt='%(ld_begin)s')
      select a.faudit_date,
             a.fgamefsk,
             a.fplatformfsk,
             a.fbpid,
             a.fuid,
             a.flts_at,
             a.fact_type,
             a.fact_id,
             a.fact_num,
             a.fuser_gamecoins_num,
             a.fseq_no,
             a.fdiff
        from (select a.*,
                     sum(a.fdiff) over(partition by a.fgamefsk, a.fplatformfsk, a.fuid order by a.flts_at, a.fseq_no rows between 5 preceding and 5 following) flag
                from stage.audit_gamecoin_error_record_tmp_%(num_date)s a
                join (select fgamefsk, fplatformfsk, fuid
                       from (select fgamefsk,
                                    fplatformfsk,
                                    fuid,
                                    row_number() over(partition by fgamefsk, fplatformfsk order by fuid) rown
                               from (select distinct fgamefsk, fplatformfsk, fuid
                                       from stage.audit_gamecoin_error_record_tmp_%(num_date)s
                                    ) t1
                            ) t
                      where rown <= 10) b
                  on a.fgamefsk = b.fgamefsk
                 and a.fplatformfsk = b.fplatformfsk
                 and a.fuid = b.fuid
           ) a
       where a.flag != 0;
        """ % args_dic

        hql += """

       drop table if exists analysis.audit_result_report_gamecoin_tmp_%(num_date)s;

       create table analysis.audit_result_report_gamecoin_tmp_%(num_date)s as
       select "%(ld_begin)s" fdate,
              a.fgamefsk,
              a.fplatformfsk,
              'gamecoin' ftype,
              cast(null as string) fsolve_at,
              a.ftotal flog_num,
              nvl(b.ferror, 0) flog_error_num,
              cast(null as varchar(4000)) fdetail,
              cast(null as varchar(4000)) fcomment,
              0 fstatus,
              cast(null as varchar(4000)) f1validation,
              cast(null as string) f1validation_at,
              cast(null as varchar(4000)) f2validation,
              cast(null as string) f2validation_at,
              nvl(b.fsystem_fault, 0) fsystem_fault
         from (select fgamefsk, fplatformfsk, count(1) ftotal
                 from stage.pb_gamecoins_stream_stg a
                 join dim.bpid_map b
                   on a.fbpid = b.fbpid
                where dt = "%(ld_begin)s"
                group by fgamefsk, fplatformfsk) a
         left join (select fgamefsk,
                           fplatformfsk,
                           count(1) ferror,
                           sum(fdiff) fsystem_fault
                      from stage.audit_gamecoin_error_record_tmp_%(num_date)s
                     where fdiff != 0
                     group by fgamefsk, fplatformfsk) b
           on a.fgamefsk = b.fgamefsk
          and a.fplatformfsk = b.fplatformfsk;
        """ % args_dic

##########################################################################
# 博雅币审计
##########################################################################
        hql += """
              use stage;

     drop table if exists stage.audit_bycoin_error_record_tmp_%(num_date)s;

     create table stage.audit_bycoin_error_record_tmp_%(num_date)s
     as
     select fgamefsk,
            fplatformfsk,
            fbpid,
            fuid,
            flts_at,
            fact_type,
            fact_id,
            fact_num,
            fuser_bycoins_num,
            fseq_no,
            fdiff,
            "%(ld_begin)s" faudit_date
       from (select fgamefsk,
                    fplatformfsk,
                    fbpid,
                    fuid,
                    flts_at,
                    fact_type,
                    fact_id,
                    fact_num,
                    fuser_bycoins_num,
                    fseq_no fseq_no,
                    fdiff,
                    sum(fdiff) over(partition by fgamefsk, fplatformfsk, fuid order by flts_at, fseq_no rows between unbounded preceding and unbounded following) diff_sum
               from (select b.fgamefsk,
                            b.fplatformfsk,
                            a.fbpid,
                            a.fuid,
                            a.flts_at,
                            a.fact_type,
                            a.fact_id,
                            a.fact_num,
                            a.fuser_bycoins_num,
                            a.fseq_no,
                            nvl(a.fuser_bycoins_num -
                                if(a.fact_type=1, 1, -1) *
                                abs(fact_num) - lag(fuser_bycoins_num)
                                over(partition by fgamefsk,
                                     fplatformfsk,
                                     fuid order by flts_at,
                                     fseq_no),
                                0) fdiff
                       from pb_bycoins_stream_stg a
                       join dim.bpid_map b
                         on a.fbpid = b.fbpid
                        and b.fgamefsk not in (4389900412,
                                               4118194431,
                                               33658545,
                                               1396899,
                                               54871823,
                                               4204924431,
                                               60321414,
                                               60321415,
                                               60321416,
                                               60321417,
                                               60321418,
                                               1731825777,
                                               1752221033,
                                               1767597564)
                      where dt >= "%(ld_begin)s"
                        and dt < "%(ld_end)s"
                        and fact_type in (1, 2)) t1

             ) t
      where diff_sum != 0;

    """ % args_dic

        hql += """
      insert overwrite table analysis.audit_bycoin_error_sample
      partition(dt='%(ld_begin)s')
      select a.faudit_date,
             a.fgamefsk,
             a.fplatformfsk,
             a.fbpid,
             a.fuid,
             a.flts_at,
             a.fact_type,
             a.fact_id,
             a.fact_num,
             a.fuser_bycoins_num,
             a.fseq_no,
             a.fdiff
        from (select a.*,
                     sum(a.fdiff) over(partition by a.fgamefsk, a.fplatformfsk, a.fuid order by a.flts_at, a.fseq_no rows between 5 preceding and 5 following) flag
                from stage.audit_bycoin_error_record_tmp_%(num_date)s a
                join (select fgamefsk, fplatformfsk, fuid
                       from (select fgamefsk,
                                    fplatformfsk,
                                    fuid,
                                    row_number() over(partition by fgamefsk, fplatformfsk order by fuid) rown
                               from (select distinct fgamefsk, fplatformfsk, fuid
                                       from stage.audit_bycoin_error_record_tmp_%(num_date)s
                                    ) t1
                            ) t
                      where rown <= 10) b
                  on a.fgamefsk = b.fgamefsk
                 and a.fplatformfsk = b.fplatformfsk
                 and a.fuid = b.fuid
           ) a
       where a.flag != 0;
       """ % args_dic

        hql += """

       drop table if exists analysis.audit_result_report_bycoin_tmp_%(num_date)s;

       create table analysis.audit_result_report_bycoin_tmp_%(num_date)s as
       select "%(ld_begin)s" fdate,
              a.fgamefsk,
              a.fplatformfsk,
              'bycoin' ftype,
              cast(null as string) fsolve_at,
              a.ftotal flog_num,
              nvl(b.ferror, 0) flog_error_num,
              cast(null as varchar(4000)) fdetail,
              cast(null as varchar(4000)) fcomment,
              0 fstatus,
              cast(null as varchar(4000)) f1validation,
              cast(null as string) f1validation_at,
              cast(null as varchar(4000)) f2validation,
              cast(null as string) f2validation_at,
              nvl(b.fsystem_fault, 0) fsystem_fault
         from (select fgamefsk, fplatformfsk, count(1) ftotal
                 from stage.pb_bycoins_stream_stg a
                 join dim.bpid_map b
                   on a.fbpid = b.fbpid
                where dt = "%(ld_begin)s"
                group by fgamefsk, fplatformfsk) a
         left join (select fgamefsk,
                           fplatformfsk,
                           count(1) ferror,
                           sum(fdiff) fsystem_fault
                      from stage.audit_bycoin_error_record_tmp_%(num_date)s
                     where fdiff != 0
                     group by fgamefsk, fplatformfsk) b
           on a.fgamefsk = b.fgamefsk
          and a.fplatformfsk = b.fplatformfsk;
              """ % args_dic

##########################################################################
# 结果汇总
##########################################################################

        hql += """
              use stage;

              insert overwrite table analysis.audit_result_report partition(dt="%(ld_begin)s")
              select *
                from (
                       select fdate, fgamefsk, fplatformfsk, ftype, fsolve_at, flog_num, flog_error_num, fdetail, fcomment, fstatus, f1validation, f1validation_at, f2validation, f2validation_at, fsystem_fault, null
                         from analysis.audit_result_report_bycoin_tmp_%(num_date)s
                       union all
                       select fdate, fgamefsk, fplatformfsk, ftype, fsolve_at, flog_num, flog_error_num, fdetail, fcomment, fstatus, f1validation, f1validation_at, f2validation, f2validation_at, fsystem_fault, null
                         from analysis.audit_result_report_gamecoin_tmp_%(num_date)s
                       ) t
               where fdate = "%(ld_begin)s";
              """ % args_dic

        # 保留一周的数据以备查询
        hql += """
        drop table if exists stage.audit_bycoin_error_record_tmp_%(del_num_date)s;
        drop table if exists stage.audit_gamecoin_error_record_tmp_%(del_num_date)s;
        drop table if exists analysis.audit_result_report_bycoin_tmp_%(num_date)s;
        drop table if exists analysis.audit_result_report_gamecoin_tmp_%(num_date)s;
        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

if __name__ == '__main__':
    a = audit_gamecoin_bycoin()
    a()
