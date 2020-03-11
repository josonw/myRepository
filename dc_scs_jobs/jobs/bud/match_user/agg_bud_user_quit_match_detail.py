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


class agg_bud_user_quit_match_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_quit_match_detail (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fparty_type           varchar(10)     comment '牌局类型',
               fpname                varchar(100)    comment '一级场次',
               fsubname              varchar(100)    comment '二级场次',
               fgsubname             varchar(100)    comment '三级场次',
               fmatch_id             varchar(100)    comment '赛事id',
               fround_num            int             comment '轮数',
               fquit_unum            bigint          comment '取消报名人数',
               fquit_cnt             bigint          comment '取消报名人次',
               fz_quit_unum          bigint          comment '主动取消报名人数',
               fz_quit_cnt           bigint          comment '主动取消报名人次',
               fb_quit_unum          bigint          comment '被动取消报名人数',
               fb_quit_cnt           bigint          comment '被动取消报名人次',
               fout_unum             bigint          comment '退赛人数',
               fout_cnt              bigint          comment '退赛人次',
               fz_out_unum           bigint          comment '主动退赛人数',
               fz_out_cnt            bigint          comment '主动退赛人次',
               fb_out_unum           bigint          comment '被动退赛人数',
               fb_out_cnt            bigint          comment '被动退赛人次',
               fdieout_unum          bigint          comment '淘汰人数',
               fdieout_cnt           bigint          comment '淘汰人次',
               f1_z_out_unum         bigint          comment '第一名主动退赛人数',
               f1_z_out_cnt          bigint          comment '第一名主动退赛人次',
               f2_z_out_unum         bigint          comment '第二名主动退赛人数',
               f2_z_out_cnt          bigint          comment '第二名主动退赛人次',
               f3_z_out_unum         bigint          comment '第三名主动退赛人数',
               f3_z_out_cnt          bigint          comment '第三名主动退赛人次',
               fgame_num             int             comment '局数'
               )comment '退赛详情'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_quit_match_detail';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fparty_type', 'fpname', 'fsubname', 'fgsubname', 'fmatch_id', 'fround_num', 'fgame_num'],
                        'groups': [[1, 1, 1, 1, 1, 1, 1],
                                   [1, 1, 1, 1, 0, 0, 0],
                                   [1, 1, 1, 1, 1, 0, 0],
                                   [1, 1, 1, 1, 0, 1, 1],
                                   [1, 0, 0, 0, 0, 0, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_quit_match_detail_tmp_1_%(statdatenum)s;
          create table work.bud_user_quit_match_detail_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t2.fparty_type,'%(null_str_report)s') fparty_type
                 ,coalesce(t2.fpname,'%(null_str_report)s') fpname
                 ,coalesce(t2.fsubname,'%(null_str_report)s') fsubname
                 ,coalesce(t2.fgsubname,'%(null_str_report)s') fgsubname
                 ,case when fparty_type = '快速赛' then cast (coalesce(t1.fmatch_cfg_id,0) as string)
                  else concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) end fmatch_id
                 ,t1.fuid
                 ,t1.fcause            --退赛原因
                 ,coalesce(t1.fround_num,0) fround_num
                 ,coalesce(t1.fgame_num,0) fgame_num
                 ,coalesce(t1.fpart_rank,%(null_int_report)d) fpart_rank
            from stage.quit_gameparty_stg t1
            left join dim.match_config t2
              on concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) = t2.fmatch_id
             and t2.dt = '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,%(null_int_group_rule)d) fparty_type
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname
                 ,coalesce(fmatch_id,'%(null_str_group_rule)s') fmatch_id
                 ,coalesce(fround_num,%(null_int_group_rule)d) fround_num
                 ,count(distinct case when fcause in (1,2) then fuid end) quit_unum       --取消报名人数
                 ,count(case when fcause in (1,2) then fuid end) fquit_cnt                --取消报名人次
                 ,count(distinct case when fcause = 1 then fuid end) fz_quit_unum         --主动取消报名人数
                 ,count(case when fcause = 1 then fuid end) fz_quit_cnt                   --主动取消报名人次
                 ,count(distinct case when fcause = 2 then fuid end) fb_quit_unum         --被动取消报名人数
                 ,count(case when fcause = 2 then fuid end) fb_quit_cnt                   --被动取消报名人次
                 ,count(distinct case when fcause in (3,4) then fuid end) fout_unum       --退赛人数
                 ,count(case when fcause in (3,4) then fuid end) fout_cnt                 --退赛人次
                 ,count(distinct case when fcause = 3 then fuid end) fz_out_unum          --主动退赛人数
                 ,count(case when fcause = 3 then fuid end) fz_out_cnt                    --主动退赛人次
                 ,count(distinct case when fcause = 4 then fuid end) fb_out_unum          --被动退赛人数
                 ,count(case when fcause = 4 then fuid end) fb_out_cnt                    --被动退赛人次
                 ,count(distinct case when fcause = 5 then fuid end) fdieout_unum         --淘汰人数
                 ,count(case when fcause = 5 then fuid end) fdieout_cnt                   --淘汰人次
                 ,count(distinct case when fcause = 3 and fpart_rank = 1 then fuid end) f1_z_out_unum        --第一名主动退赛人数
                 ,count(case when fcause = 3 and fpart_rank = 1 then fuid end) f1_z_out_cnt                  --第一名主动退赛人次
                 ,count(distinct case when fcause = 3 and fpart_rank = 2 then fuid end) f2_z_out_unum        --第二名主动退赛人数
                 ,count(case when fcause = 3 and fpart_rank = 2 then fuid end) f2_z_out_cnt                  --第二名主动退赛人次
                 ,count(distinct case when fcause = 3 and fpart_rank = 3 then fuid end) f3_z_out_unum        --第三名主动退赛人数
                 ,count(case when fcause = 3 and fpart_rank = 3 then fuid end) f3_z_out_cnt                  --第三名主动退赛人次
                 ,coalesce(fgame_num,%(null_int_group_rule)d) fgame_num
            from work.bud_user_quit_match_detail_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type, fpname, fsubname, fgsubname, fmatch_id, fround_num, fgame_num
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_quit_match_detail
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_quit_match_detail_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_quit_match_detail(sys.argv[1:])
a()
