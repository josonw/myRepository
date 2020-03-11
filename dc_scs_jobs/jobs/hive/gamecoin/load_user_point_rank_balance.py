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


class load_user_point_rank_balance(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists dim.user_point_rank_balance_day (
               fdate               string,
               fbpid               varchar(50),
               fgame_id            bigint,
               fuid                bigint,
               fpoint              bigint     comment '积分',
               frank               bigint     comment '段位',
               fparty_num          bigint     comment '牌局数',
               fwin_party_num      bigint     comment '胜局数',
               fwin_point          bigint     comment '胜点'
               )comment '用户积分结余'
               partitioned by(dt date)
        location '/dw/dim/user_point_rank_balance_day';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        create table if not exists dim.user_point_rank_balance (
               fdate               string,
               fbpid               varchar(50),
               fsubgamefsk         bigint,
               fuid                bigint,
               fpoint              bigint     comment '积分',
               frank               bigint     comment '段位',
               fparty_num          bigint     comment '牌局数',
               fwin_party_num      bigint     comment '胜局数',
               fparty_num_all      bigint     comment '总牌局数',
               fwin_party_num_all  bigint     comment '总胜局数',
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fwin_point          bigint     comment '胜点'
               )comment '用户积分结余'
               partitioned by(dt string)
        stored as parquet;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set mapreduce.map.memory.mb=2048;set mapreduce.map.java.opts=-XX:-UseGCOverheadLimit -Xmx1700m;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--积分_当日
        insert overwrite table dim.user_point_rank_balance_day
        partition(dt="%(statdate)s")
        select  t1.flts_at fdate,
                t1.fbpid,
                coalesce(t1.fgame_id, %(null_int_report)d) fgame_id,
                t1.fuid,
                t1.fpoint,
                t1.frank,
                t2.fparty_num,
                t2.fwin_party_num,
                t1.fwin_point
           from ( select *
                    from (select fbpid,
                                 coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                                 fuid,
                                 flts_at,
                                 fpoint,
                                 frank,
                                 fwin_point,
                                 row_number() over(partition by fbpid, fgame_id, fuid order by flts_at desc, fpoint desc,fwin_point desc) row_num
                            from stage.user_point_rank_stg
                           where dt = "%(statdate)s"
                         ) t1
                   where row_num = 1
                ) t1
           left join (select fbpid,
                             coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                             fuid,
                             count(fuid) fparty_num,       --牌局数
                             count(case when fact_type = 1 then fuid end) fwin_party_num   --胜局数
                        from stage.user_point_rank_stg t
                       where dt = "%(statdate)s"
                       group by fbpid,
                                fgame_id,
                                fuid
                     ) t2
                  on t1.fbpid = t2.fbpid
                 and t1.fuid = t2.fuid
                 and t1.fgame_id = t2.fgame_id;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """-- 积分_全量
        insert overwrite table dim.user_point_rank_balance
        partition(dt="%(statdate)s")
        select  fdate,
                fbpid,
                fgame_id fsubgamefsk,
                fuid,
                fpoint,
                frank,
                fparty_num,
                fwin_party_num,
                fparty_num_all,
                fwin_party_num_all,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fterminaltypefsk,
                fversionfsk,
                fwin_point
          from ( select fdate,t.fbpid,fgame_id,fuid,fpoint,frank,fwin_point,fparty_num,fwin_party_num,fparty_num_all,fwin_party_num_all,
                        tt.fgamefsk,tt.fplatformfsk,tt.fhallfsk,tt.fterminaltypefsk,tt.fversionfsk,
                        row_number() over(partition by t.fbpid, fgame_id, fuid order by fdate desc, fpoint desc,fwin_point desc) row_num
                   from (select fdate,fbpid,fsubgamefsk fgame_id,fuid,fpoint,frank,fwin_point,fparty_num,fwin_party_num,fparty_num_all,fwin_party_num_all
                           from dim.user_point_rank_balance t1
                          where dt = date_sub("%(statdate)s", 1)

                          union all

                         select t1.fdate,
                                t1.fbpid,
                                t1.fgame_id,
                                t1.fuid,
                                t1.fpoint,
                                t1.frank,
                                t1.fwin_point,
                                t1.fparty_num,
                                t1.fwin_party_num,
                                t1.fparty_num + t2.fparty_num_all fparty_num_all,
                                t1.fwin_party_num + t2.fwin_party_num_all fwin_party_num_all
                           from dim.user_point_rank_balance_day t1
                           left join dim.user_point_rank_balance t2
                             on t1.fbpid = t2.fbpid
                            and t1.fuid = t2.fuid
                            and t1.fgame_id = t2.fsubgamefsk
                            and t2.dt = date_sub("%(statdate)s", 1)
                          where t1.dt = "%(statdate)s"
                        ) t
                     join dim.bpid_map_bud tt
                       on t.fbpid = tt.fbpid
               ) tt
         where row_num = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_point_rank_balance(sys.argv[1:])
a()
