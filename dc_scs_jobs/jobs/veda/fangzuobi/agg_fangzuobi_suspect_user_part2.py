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


class agg_fangzuobi_suspect_user_part2(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """-- 数据准备，异常行为用户
insert overwrite table veda.tmp_suspect_behavior_user  partition(dt='%(statdate)s')
select 1, a.fuid
  from (select m.fuid
          from (select distinct x.fuid,
                       case when split(x.fsubname, '_')[1] = '100' then split(x.fsubname, '_')[1] else split(x.fsubname, '_')[0] end
                  from stage.user_gameparty_stg x
                  left join dim.bpid_map y
                    on x.fbpid = y.fbpid
                 where y.fgamename = '地方棋牌'
                   and x.dt = '%(statdate)s'
                   and (x.fsubname like '%%\_100' or x.fsubname like 'LV3\_%%')
               ) m
         group by m.fuid
        having count(*) = 2
       ) a
  join (select q.fuid
          from (select distinct o.fuid,
                       case when split(o.fsubname, '_')[1] = '100' then split(o.fsubname, '_')[1] else split(o.fsubname, '_')[0] end party
                  from stage.user_gameparty_stg o
                  left join dim.bpid_map p
                    on o.fbpid = p.fbpid
                 where p.fgamename = '地方棋牌'
                   and o.dt = '%(statdate)s'
               ) q
         group by q.fuid
        having count(*) = 2
       ) b
    on a.fuid = b.fuid
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- 数据准备，异常行为用户
insert into table veda.tmp_suspect_behavior_user  partition(dt='%(statdate)s')
select 2, a.fuid
  from (select m.fuid
          from (select distinct x.fuid,
                       case when split(x.fsubname, '_')[1] = '100' then split(x.fsubname, '_')[1] else split(x.fsubname, '_')[0] end
                  from stage.user_gameparty_stg x
                  left join dim.bpid_map y
                    on x.fbpid = y.fbpid
                 where y.fgamename = '地方棋牌'
                   and x.dt = '%(statdate)s'
                   and (x.fsubname like '%%\_100' or x.fsubname like 'LV23\_%%')
               ) m
         group by m.fuid
        having count(*) = 2
       ) a
  join (select q.fuid
          from (select distinct o.fuid,
                       case when split(o.fsubname, '_')[1] = '100' then split(o.fsubname, '_')[1] else split(o.fsubname, '_')[0] end party
                  from stage.user_gameparty_stg o
                  left join dim.bpid_map p
                    on o.fbpid = p.fbpid
                 where p.fgamename = '地方棋牌'
                   and o.dt = '%(statdate)s'
               ) q
         group by q.fuid
        having count(*) = 2
       ) b
    on a.fuid = b.fuid
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- 数据准备，异常行为用户
insert into table veda.tmp_suspect_behavior_user  partition(dt='%(statdate)s')
select 3, a.fuid
  from (select m.fuid
          from (select distinct x.fuid,
                       case when split(x.fsubname, '_')[1] = '100' then split(x.fsubname, '_')[1] else split(x.fsubname, '_')[0] end
                  from stage.user_gameparty_stg x
                  left join dim.bpid_map y
                    on x.fbpid = y.fbpid
                 where y.fgamename = '地方棋牌'
                   and x.dt = '%(statdate)s'
                   and (x.fsubname like '%%\_100' or x.fsubname like 'LV10\_%%')
               ) m
         group by m.fuid
        having count(*) = 2
       ) a
  join (select q.fuid
          from (select distinct o.fuid,
                       case when split(o.fsubname, '_')[1] = '100' then split(o.fsubname, '_')[1] else split(o.fsubname, '_')[0] end party
                  from stage.user_gameparty_stg o
                  left join dim.bpid_map p
                    on o.fbpid = p.fbpid
                 where p.fgamename = '地方棋牌'
                   and o.dt = '%(statdate)s'
               ) q
         group by q.fuid
        having count(*) = 2
       ) b
    on a.fuid = b.fuid
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- 数据准备，异常行为用户
insert into table veda.tmp_suspect_behavior_user  partition(dt='%(statdate)s')
select distinct 4, fuid
  from stage.user_gameparty_stg x
  left join dim.bpid_map y
    on x.fbpid = y.fbpid
 where y.fgamename = '地方棋牌'
   and x.dt = '%(statdate)s'
   and (x.fuser_gcoins - x.fgamecoins + x.fcharge ) <= 50000
   and x.fsubname like '%%\_100'
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- 数据准备，异常行为用户
insert into table veda.tmp_suspect_behavior_user  partition(dt='%(statdate)s')
select 5, o.fuid
  from (select m.fuid
          from (select distinct x.fuid, x.act_id
                  from stage.pb_gamecoins_stream_stg x
                  left join dim.bpid_map y
                    on x.fbpid = y.fbpid
                 where y.fgamename = '地方棋牌'
                   and x.dt = '%(statdate)s'
                   and x.act_id in (100, 101)
               ) m
         group by m.fuid
        having count(*) = 2
       ) o
  join (select n.fuid
          from (select distinct a.fuid, a.act_id
                  from stage.pb_gamecoins_stream_stg a
                  left join dim.bpid_map b
                    on a.fbpid = b.fbpid
                 where b.fgamename = '地方棋牌'
                   and a.dt = '%(statdate)s'
               ) n
         group by n.fuid
        having count(*) = 2
       ) p
    on o.fuid = p.fuid;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table veda.tmp_suspect_behavior_ip_user partition(dt='%(statdate)s')
select n.fbehavior_type, m.fip, m.fuid
  from (select distinct fuid, fip
          from stage.user_login_stg x
          left join dim.bpid_map y
            on x.fbpid = y.fbpid
         where y.fgamename = '地方棋牌'
           and x.dt = '%(statdate)s'
       ) m
  join (select fuid, fbehavior_type
          from veda.tmp_suspect_behavior_user
         where dt = '%(statdate)s'
           and fbehavior_type in (1, 2, 3)
       ) n
    on m.fuid = n.fuid
 union all
select 4, m.fip, m.fuid
  from (select distinct fuid, fip
          from stage.user_login_stg x
          left join dim.bpid_map y
            on x.fbpid = y.fbpid
         where y.fgamename = '地方棋牌'
           and x.dt = '%(statdate)s'
       ) m
  join (select fuid, fbehavior_type
          from veda.tmp_suspect_behavior_user
         where dt = '%(statdate)s'
           and fbehavior_type = 4
       ) n
    on m.fuid = n.fuid
 union all
select 5, m.fip, m.fuid
  from (select distinct fuid, fip
          from stage.user_login_stg x
          left join dim.bpid_map y
            on x.fbpid = y.fbpid
         where y.fgamename = '地方棋牌'
           and x.dt = '%(statdate)s'
       ) m
  join (select fuid, fbehavior_type
          from veda.tmp_suspect_behavior_user
         where dt = '%(statdate)s'
           and fbehavior_type = 5
       ) n
    on m.fuid = n.fuid
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- d5:与同IP下≥10个账号同一天都只进入过斗牛和私人房
insert overwrite table veda.suspect_user_d5
partition(dt='%(statdate)s')
select distinct fuid, fip
  from veda.tmp_suspect_behavior_ip_user
 where dt = '%(statdate)s'
   and fbehavior_type = 1
   and fip in (select a.fip
                 from veda.tmp_suspect_behavior_ip_user a
                where a.dt = '%(statdate)s'
                  and a.fbehavior_type = 1
                group by a.fip
               having count(*) >= 10)
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- d6:与同IP下≥10个账号同一天都只进入过焖鸡和私人房
insert overwrite table veda.suspect_user_d6 partition(dt='%(statdate)s')
select distinct fuid, fip
  from veda.tmp_suspect_behavior_ip_user
 where dt = '%(statdate)s'
   and fbehavior_type = 2
   and fip in (select a.fip
                 from veda.tmp_suspect_behavior_ip_user a
                where a.dt = '%(statdate)s'
                  and a.fbehavior_type = 2
                group by a.fip
               having count(*) >= 10)
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """

-- d7:与同IP下≥10个账号同一天都只进入过马股和私人房
insert overwrite table veda.suspect_user_d7 partition(dt='%(statdate)s')
select distinct fuid, fip
  from veda.tmp_suspect_behavior_ip_user
 where dt = '%(statdate)s'
   and fbehavior_type = 3
   and fip in (select a.fip
                 from veda.tmp_suspect_behavior_ip_user a
                where a.dt = '%(statdate)s'
                  and a.fbehavior_type = 3
                group by a.fip
               having count(*) >= 10)
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- d8:与同IP下≥5个账号有至少1次进入私人房的资产≤50000
insert overwrite table veda.suspect_user_d8 partition(dt='%(statdate)s')
select distinct fuid, fip
  from veda.tmp_suspect_behavior_ip_user
 where dt = '%(statdate)s'
   and fbehavior_type = 4
   and fip in (select a.fip
                 from veda.tmp_suspect_behavior_ip_user a
                where a.dt = '%(statdate)s'
                  and a.fbehavior_type = 4
                group by a.fip
               having count(*) >= 5)
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
-- d9:与同IP下≥10个账号注册后只领取了注册金币和登录金币
insert overwrite table veda.suspect_user_d9 partition(dt='%(statdate)s')
select distinct fuid, fip
  from veda.tmp_suspect_behavior_ip_user
 where dt = '%(statdate)s'
   and fbehavior_type = 5
   and fip in (select a.fip
                 from veda.tmp_suspect_behavior_ip_user a
                where a.dt = '%(statdate)s'
                  and a.fbehavior_type = 5
                group by a.fip
               having count(*) >= 10)
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_part2(sys.argv[1:])
a()
