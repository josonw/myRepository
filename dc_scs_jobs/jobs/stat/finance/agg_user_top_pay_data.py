#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经注释不使用
class agg_user_top_pay_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_top_pay_fct
        (
            fdate               date,
            fgamefsk            bigint,
            fplatformfsk        bigint,
            fversionfsk         bigint,
            fterminalfsk        bigint,
            fuid                bigint,
            fplatform_uid       string,
            ftoppay_cnt         decimal(20,2),
            frank               bigint,
            fuser_age           bigint,
            fentrance_id        bigint,
            fpaycnt             bigint,
            fentrance_name      varchar(100),
            fregister_date      string,
            ftop_paycnt         bigint,
            flast_active_date   string
        )
        partitioned by (dt date);
        create table if not exists analysis.user_top_pay_fct_tmp
        (
            fbpid               string,
            fuid                bigint,
            fplatform_uid       string,
            ftoppay_cnt         decimal(20,2),
            frank               bigint,
            fuser_age           bigint,
            fentrance_id        bigint,
            fpaycnt             bigint,
            fentrance_name      string,
            fregister_date      string,
            ftop_paycnt         bigint,
            flast_active_date   string
        );

        create table if not exists analysis.user_toppay_fsk
        (
            fsk             bigint,
            fuid            bigint,
            fgamefsk        bigint,
            fplatformfsk    bigint,
            fplatform_uid   string
        );

        create table if not exists analysis.user_top_pay_actday
        (
            fdate           date,
            fgamefsk        bigint,
            fplatformfsk    bigint,
            fversionfsk     bigint,
            fterminalfsk    bigint,
            fuid            bigint,
            fplatform_uid   string,
            factday         bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        truncate table user_top_pay_fct_tmp
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.user_top_pay_fct_tmp
       select a.fbpid,
             b.fuid,
             a.fplatform_uid,
             a.dip           ftoppay_cnt,
             a.rown          frank,
            null fuser_age,
            null fentrance_id,
            null fpaycnt,
            null fentrance_name,
            null fregister_date,
            null ftop_paycnt,
            null flast_active_date
        from (select *
                from (select a.fbpid,
                             a.fplatform_uid,
                             sum(round(fcoins_num * frate, 2)) dip,
                             row_number() over(partition by a.fbpid order by sum(round(fcoins_num * frate, 2)) desc) rown
                        from stage.payment_stream_stg a
                       where a.dt >= '%(ld_1month_ago_begin)s'
                         and a.dt < '%(ld_month_begin)s'
                       group by a.fbpid, a.fplatform_uid) a
               where rown <= 100) a
        left join (select fbpid, fuid, fplatform_uid
                    from stage.user_pay_info b
                   where b.fpay_at >= '%(ld_1month_ago_begin)s'
                     and fpay_at < '%(ld_month_begin)s'
                   group by fbpid, fuid, fplatform_uid) b
          on a.fbpid = b.fbpid
         and a.fplatform_uid = b.fplatform_uid;
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        -- top 100 新加 用户类型，付费总次数，用户年龄 类型
        insert into table analysis.user_top_pay_fct_tmp
        select
        tp.fbpid,
        tp.fuid,
        tp.fplatform_uid,
        null ftoppay_cnt,
        null frank,
        max(datediff('%(stat_date)s', ud.dt)) fuser_age,
        null fentrance_id,
        count(ps.forder_id)                  fpaycnt,
        null fentrance_name,
        null fregister_date,
        null ftop_paycnt,
        null flast_active_date
         from analysis.user_top_pay_fct_tmp tp
         left join stage.user_dim ud
           on tp.fbpid = ud.fbpid
          and tp.fuid = ud.fuid
          and tp.fplatform_uid = ud.fplatform_uid
         left join stage.payment_stream_stg ps
           on tp.fbpid = ps.fbpid
          and tp.fplatform_uid = ps.fplatform_uid
        where ps.fdate < date_add('%(stat_date)s', 1)
        group by tp.fbpid,
                 tp.fuid,
                 tp.fplatform_uid
        """% self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_top_pay_fct partition
        (dt = "%(ld_1month_ago_begin)s")
         select
            '%(ld_1month_ago_begin)s' fdate,
            b.fgamefsk,
            b.fplatformfsk,
            b.fversionfsk,
            b.fterminalfsk,
            fuid,
            fplatform_uid,
            max(ftoppay_cnt)            ftoppay_cnt,
            max(frank)                  frank,
            max(fuser_age)              fuser_age,
            max(fentrance_id)           fentrance_id,
            max(fpaycnt)                fpaycnt,
            max(fentrance_name)         fentrance_name,
            max(fregister_date)         fregister_date,
            max(ftop_paycnt)            ftop_paycnt,
            max(flast_active_date)      flast_active_date
        from analysis.user_top_pay_fct_tmp a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        group by
            b.fgamefsk,
            b.fplatformfsk,
            b.fversionfsk,
            b.fterminalfsk,
            fuid,
            fplatform_uid
        """ % self.hql_dict
        hql_list.append( hql )


        # fuck fsk
        # 目前还不是怎么处理
        hql = """
        merge into analysis.user_toppay_fsk a
        using (select a.fplatform_uid, a.fgamefsk, a.fplatformfsk
                 from analysis.user_top_pay_fct a
                where a.fdate = '%(stat_date)s'
                group by a.fplatform_uid, a.fgamefsk, a.fplatformfsk) b
        on (a.fplatform_uid = b.fplatform_uid and a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk)
        when not matched then
          insert
            (a.fsk, a.fplatform_uid, a.fgamefsk, a.fplatformfsk)
          values
            (analysis.common_seq.nextval,
             b.fplatform_uid,
             b.fgamefsk,
             b.fplatformfsk);
        commit;
        """

        hql = """
        insert overwrite table analysis.user_top_pay_actday partition
        (dt = "%(stat_date)s")
          select '%(stat_date)s' fdate,
           a.fgamefsk,
           a.fplatformfsk,
           a.fversionfsk,
           a.fterminalfsk,
           a.fuid,
           a.fplatform_uid,
           datediff('%(stat_date)s', max(d.fdate) ) factday
            from analysis.user_top_pay_fct a
            left join analysis.bpid_platform_game_ver_map c
              on a.fgamefsk = c.fgamefsk
             and a.fplatformfsk = c.fplatformfsk
             and a.fversionfsk = c.fversionfsk
             and a.fterminalfsk = c.fterminalfsk
            left join stage.active_user_mid d
              on d.fbpid = c.fbpid
             and a.fuid = d.fuid
           where a.fdate = '%(ld_1month_ago_begin)s'
             and d.fdate >= '%(ld_1month_ago_begin)s'
             and d.fdate < date_add('%(stat_date)s', 1)
           group by a.fgamefsk,
                    a.fplatformfsk,
                    a.fversionfsk,
                    a.fterminalfsk,
                    a.fuid,
                    a.fplatform_uid
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
    a = agg_user_top_pay_data(stat_date)
    a()
