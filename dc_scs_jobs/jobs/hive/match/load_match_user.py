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

class load_match_user(BaseStatModel):
    def create_tab(self):
        """ 比赛用户中间表
            2017-09-04 修改将 match_cfg_id与match_log_id组合成match_id"""
        hql = """create table if not exists dim.match_user
            (
             fbpid         varchar(50)     comment  'bpid',
             fgame_id      bigint          comment  '子游戏id',
             fuid          bigint          comment  '用户id',
             fmatch_id     varchar(100)    comment  '赛事id',
             fpname        varchar(100)    comment  '一级场次',
             fsubname      varchar(100)    comment  '二级场次',
             fgsubname     varchar(128)    comment  '三级场次',
             fparty_type   varchar(100)    comment  '比赛类型',
             fmode         varchar(100)    comment  '报名模式',
             fitem_id      varchar(100)    comment  '物品id',
             fentry_fee    bigint          comment  '报名费',
             fcause        int             comment  '退赛原因',
             fio_type      int             comment  '发放消耗',
             ffirst_apply  int             comment  '是否首次报名',
             fnew_apply    int             comment  '是否注册用户报名',
             ffirst_party  int             comment  '是否首次参赛',
             fnew_party    int             comment  '是否注册用户参赛',
             fitem_num     bigint          comment  '物品数量',
             fcost         decimal(30,2)   comment  '物品成本RMB',
             fjoin_flag    int             comment  '报名标识',
             fparty_flag   int             comment  '参赛标识',
             fquit_flag    int             comment  '退赛标识',
             faward_type   string          comment  '奖励类型'
            )
            partitioned by (dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分,统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.match_user partition (dt = "%(statdate)s" )
          select a.fbpid,
                 coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                 fuid,
                 a.fmatch_id,
                 coalesce(b.fpname,c.fpname,'%(null_str_report)s') fpname,
                 coalesce(b.fname,c.fsubname,'%(null_str_report)s') fsubname,
                 coalesce(b.fsubname,c.fgsubname,'%(null_str_report)s') fgsubname,
                 coalesce(b.fparty_type,c.fparty_type,'%(null_str_report)s') fparty_type,
                 coalesce(b.fmode,'%(null_str_report)s') fmode,
                 fitem_id,
                 sum(nvl(fentry_fee,0)) fentry_fee,
                 fcause,    --退赛原因
                 fio_type,  --发放消耗
                 max(ffirst_apply) ffirst_apply, --首次报名
                 max(fnew_apply) fnew_apply,     --大厅新用户报名
                 max(ffirst_party) ffirst_party, --首次参赛
                 max(fnew_party) fnew_party,     --大厅新用户参赛
                 sum(fitem_num) fitem_num,       --发放消耗数额
                 sum(nvl(fcost,cast (0 as decimal(30,2)))) fcost,        --成本(现金价值)
                 max(fjoin_flag) fjoin_flag,     --报名标志，默认所有出现的都报过名
                 max(fparty_flag) fparty_flag,
                 max(fquit_flag) fquit_flag,
                 coalesce(b.faward_type,c.faward_type,'%(null_str_report)s') faward_type
               --  sum(case when fcause >0 then 0 else 1 end) over(partition by fmatch_id) quie_id  --退赛标志，0为退赛，1为为退赛，如果一个match_id全为0则开赛失败
            from (
                --报名参加牌局比赛
                  select jg.fbpid,
                         coalesce(jg.fgame_id,%(null_int_report)d) fgame_id,
                         jg.fuid,
                         concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id,
                         jg.fitem_id,
                         jg.fentry_fee,
                         0 fcause,
                         null fio_type,
                         ffirst_match ffirst_apply,  --首次 报名
                         case when ru.fuid is not null then 1 else 0 end fnew_apply, --大厅新用户 报名
                         0 ffirst_party,
                         0 fnew_party,
                         0 fitem_num,
                         cast (0 as decimal(30,2)) fcost,
                         1 fjoin_flag,
                         0 fparty_flag,
                         0 fquit_flag
                    from stage.join_gameparty_stg jg
                    left join dim.reg_user_main_additional ru
                      on jg.fbpid = ru.fbpid
                     and jg.fuid = ru.fuid
                     and ru.dt = '%(statdate)s'
                   where jg.dt='%(statdate)s'
                     and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                   union all
                --参赛
                  select ug.fbpid,
                         coalesce(ug.fgame_id,%(null_int_report)d) fgame_id,
                         ug.fuid,
                         concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id,
                         null fitem_id,
                         0 fentry_fee,
                         0 fcause,
                         null fio_type,
                         0 ffirst_apply,
                         0 fnew_apply,
                         ffirst_match ffirst_party,  --首次参赛
                         case when ru.fuid is not null then 1 else 0 end fnew_party,  --大厅新用户参赛
                         0 fitem_num,
                         0 fcost,
                         0 fjoin_flag,
                         1 fparty_flag,
                         0 fquit_flag
                    from stage.user_gameparty_stg ug
                    left join dim.reg_user_main_additional ru
                      on ug.fbpid = ru.fbpid
                     and ug.fuid = ru.fuid
                     and ru.dt = '%(statdate)s'
                   where ug.dt='%(statdate)s'
                     and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                   union all
                --退赛
                  select fbpid,
                         coalesce(qg.fgame_id,%(null_int_report)d) fgame_id,
                         fuid,
                         concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id,
                         null fitem_id,
                         0 fentry_fee,
                         fcause,
                         null fio_type,
                         0 ffirst_apply,
                         0 fnew_apply,
                         0 ffirst_party,
                         0 fnew_party,
                         0 fitem_num,
                         0 fcost,
                         0 fjoin_flag,
                         0 fparty_flag,
                         1 fquit_flag
                    from stage.quit_gameparty_stg qg
                   where qg.dt='%(statdate)s'
                     and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                   union all
                --比赛场发放消耗
                  select fbpid,
                         coalesce(mg.fgame_id,%(null_int_report)d) fgame_id,
                         fuid,
                         concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id,
                         fitem_id,
                         0 fentry_fee,
                         0 fcause,
                         fio_type,
                         0 ffirst_apply,
                         0 fnew_apply,
                         0 ffirst_party,
                         0 fnew_party,
                         fitem_num,
                         fcost,
                         0 fjoin_flag,
                         0 fparty_flag,
                         0 fquit_flag
                    from stage.match_economy_stg mg
                   where mg.dt='%(statdate)s'
                     and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                 ) a
            left join (select distinct concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id, fpname, fname, fsubname, fparty_type, fmode, faward_type
                         from stage.join_gameparty_stg jg
                        where jg.dt='%(statdate)s'
                          and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                      ) b
              on a.fmatch_id = b.fmatch_id
            left join (select distinct concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id, fpname, fsubname, fgsubname, fparty_type, faward_type
                         from stage.user_gameparty_stg jg
                        where jg.dt='%(statdate)s'
                          and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                      ) c
              on a.fmatch_id = c.fmatch_id
           group by fbpid,
                    fgame_id,
                    fuid,
                    a.fmatch_id,
                    coalesce(b.fpname,c.fpname,'%(null_str_report)s'),
                    coalesce(b.fname,c.fsubname,'%(null_str_report)s'),
                    coalesce(b.fsubname,c.fgsubname,'%(null_str_report)s'),
                    coalesce(b.fparty_type,c.fparty_type,'%(null_str_report)s'),
                    coalesce(b.faward_type,c.faward_type,'%(null_str_report)s'),
                    coalesce(b.fmode,'%(null_str_report)s'),
                    fitem_id,
                    fcause,
                    fio_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_match_user(sys.argv[1:])
a()
