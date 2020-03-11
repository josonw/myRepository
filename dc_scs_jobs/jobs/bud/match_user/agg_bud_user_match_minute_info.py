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


class agg_bud_user_match_minute_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_minute_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fparty_type           varchar(10)      comment '牌局类型',
               fpname                varchar(100)     comment '一级场次',
               fsubname              varchar(100)     comment '二级场次',
               fgsubname             varchar(100)     comment '三级场次',
               fmatch_id             varchar(100)     comment '赛事id',
               fminute               varchar(100)     comment '分钟',
               fin_unum              bigint           comment '增加人数',
               fout_unum_1           bigint           comment '减少人数_主动取消',
               fout_unum_2           bigint           comment '减少人数_被动取消',
               fout_unum_3           bigint           comment '减少人数_主动退赛',
               fout_unum_4           bigint           comment '减少人数_被动退赛',
               fout_unum_5           bigint           comment '减少人数_淘汰',
               fin_num               bigint           comment '增加人次',
               fout_num_1            bigint           comment '减少人次_主动取消',
               fout_num_2            bigint           comment '减少人次_被动取消'
               )comment '赛事分时段人数'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_minute_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fparty_type', 'fpname', 'fsubname', 'fgsubname', 'fmatch_id', 'fminute'],
                        'groups': [[1, 1, 1, 1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_match_minute_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_match_minute_info_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.hallmode
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fmatch_id
                 ,t1.fuid
                 ,coalesce(b.fpname,c.fpname,'%(null_str_report)s') fpname
                 ,coalesce(b.fname,c.fsubname,'%(null_str_report)s') fsubname
                 ,coalesce(b.fsubname,c.fgsubname,'%(null_str_report)s') fgsubname
                 ,coalesce(b.fparty_type,c.fparty_type,'%(null_str_report)s') fparty_type
                 ,t1.fminute
                 ,t1.type
            from (select fbpid
                         ,fuid
                         ,fgame_id
                         ,fmatch_id
                         ,fminute
                         ,type
                    from (select t1.fbpid,
                                 fuid,
                                 coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                                 case when fparty_type = '快速赛' then cast (coalesce(t1.fmatch_cfg_id,0) as string)
                                 else concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) end fmatch_id,
                                 concat(hour(flts_at),':',minute(flts_at)) fminute,
                                 0 type
                            from stage.join_gameparty_stg t1
                            join dim.bpid_map tt
                              on t1.fbpid = tt.fbpid
                           where dt = '%(statdate)s'
                             and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                           union all
                          select t1.fbpid,
                                 fuid,
                                 coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                                 case when fsubname = '快速赛' then cast (coalesce(t1.fmatch_cfg_id,0) as string)
                                 else concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) end fmatch_id,
                                 concat(hour(flts_at),':',minute(flts_at)) fminute,
                                 fcause type --退赛原因
                            from stage.quit_gameparty_stg t1
                            join dim.bpid_map tt
                              on t1.fbpid = tt.fbpid
                           where t1.dt = '%(statdate)s'
                             and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                         ) t
                   group by fbpid,fuid,fgame_id,fmatch_id,fminute,type
                 ) t1
            left join (select distinct
                                 case when fparty_type = '快速赛' then cast (coalesce(t1.fmatch_cfg_id,0) as string)
                                 else concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) end fmatch_id,
                                 fpname, fname, fsubname, fparty_type
                         from stage.join_gameparty_stg t1
                        join dim.bpid_map tt
                          on t1.fbpid = tt.fbpid
                       where dt = '%(statdate)s'
                          and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                      ) b
              on t1.fmatch_id = b.fmatch_id
            left join (select distinct
                                 case when fparty_type = '快速赛' then cast (coalesce(t1.fmatch_cfg_id,0) as string)
                                 else concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) end fmatch_id,
                                 fpname, fsubname, fgsubname, fparty_type
                         from stage.user_gameparty_stg t1
                        join dim.bpid_map tt
                          on t1.fbpid = tt.fbpid
                       where dt = '%(statdate)s'
                          and coalesce(fmatch_id,'0')<>'0'
                      ) c
              on t1.fmatch_id = c.fmatch_id
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1 取报名的数据
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                 ,fmatch_id   --赛事id
                 ,fminute     --分钟
                 ,count(distinct case when type = 0 then fuid end) fin_unum      --增加人数
                 ,count(distinct case when type = 1 then fuid end) fout_unum_1   --减少人数_主动取消
                 ,count(distinct case when type = 2 then fuid end) fout_unum_2   --减少人数_被动取消
                 ,count(distinct case when type = 3 then fuid end) fout_unum_3   --减少人数_主动退赛
                 ,count(distinct case when type = 4 then fuid end) fout_unum_4   --减少人数_被动退赛
                 ,count(distinct case when type = 5 then fuid end) fout_unum_5   --减少人数_淘汰
                 ,count(case when type = 0 then fuid end) fin_num       --增加人次
                 ,count(case when type = 1 then fuid end) fout_num_1    --减少人次_主动取消
                 ,count(case when type = 2 then fuid end) fout_num_2    --减少人次_被动取消
            from work.bud_user_match_minute_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type,fpname,fsubname,fgsubname,fmatch_id,fminute
                    """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """
            insert overwrite table bud_dm.bud_user_match_minute_info
                   partition(dt='%(statdate)s')
                      %(sql_template)s """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_minute_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_minute_info(sys.argv[1:])
a()
