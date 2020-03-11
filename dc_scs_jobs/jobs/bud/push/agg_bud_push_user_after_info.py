#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_bud_push_user_after_info(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS bud_dm.bud_push_after_info(
                fdate date,
                fpush_id int COMMENT '推送id',
                act_unum_day1 int COMMENT '推送后当日活跃总人数',
                act_unum_day2 int COMMENT '推送后第2日活跃总人数',
                act_unum_day3 int COMMENT '推送后第3日活跃总人数',
                act_unum_day4 int COMMENT '推送后第4日活跃总人数',
                act_unum_day5 int COMMENT '推送后第5日活跃总人数',
                act_unum_day6 int COMMENT '推送后第6日活跃总人数',
                act_unum_day7 int COMMENT '推送后第7日活跃总人数',
                play_unum_day1 int COMMENT '推送后当日玩牌总人数',
                play_unum_day2 int COMMENT '推送后第2日玩牌总人数',
                play_unum_day3 int COMMENT '推送后第3日玩牌总人数',
                play_unum_day4 int COMMENT '推送后第4日玩牌总人数',
                play_unum_day5 int COMMENT '推送后第5日玩牌总人数',
                play_unum_day6 int COMMENT '推送后第6日玩牌总人数',
                play_unum_day7 int COMMENT '推送后第7日玩牌总人数',
                act_unum_all int COMMENT '推送后活跃总人数',
                play_unum_all int COMMENT '推送后玩牌总人数')
            COMMENT '推送后7天用户活跃和玩牌人数'
            partitioned by (dt string);

            CREATE TABLE IF NOT EXISTS bud_dm.bud_push_after_gameparty_detail(
                fdate date,
                fgamefsk bigint,
                fpush_id int COMMENT '推送id',
                fsubname int COMMENT '二级场次名称',
                play_unum_day1 int COMMENT '推送后当日场次总人数',
                play_unum_day2 int COMMENT '推送后第2日场次总人数',
                play_unum_day3 int COMMENT '推送后第3日场次总人数',
                play_unum_day4 int COMMENT '推送后第4日场次总人数',
                play_unum_day5 int COMMENT '推送后第5日场次总人数',
                play_unum_day6 int COMMENT '推送后第6日场次总人数',
                play_unum_day7 int COMMENT '推送后第7日场次总人数',
                partynum_day1 int COMMENT '推送后当日场次牌局数',
                partynum_day2 int COMMENT '推送后第2日场次牌局数',
                partynum_day3 int COMMENT '推送后第3日场次牌局数',
                partynum_day4 int COMMENT '推送后第4日场次牌局数',
                partynum_day5 int COMMENT '推送后第5日场次牌局数',
                partynum_day6 int COMMENT '推送后第6日场次牌局数',
                partynum_day7 int COMMENT '推送后第7日场次牌局数')
            COMMENT '推送后7天用户玩牌场次人数和牌局数'
            partitioned by (dt string);
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        hql = """
            with active_user_num as
             (select t1.fpush_id,
                     t2.fuid,
                     to_date(t1.fbegin_time) as push_date,
                     t3.dt as dt,
                     t3.fparty_num as fparty_num
                from stage.push_config_stg t1
               inner join dim.user_push_report t2
                  on t1.fpush_id = t2.fpush_id
                 and t2.faction = 1
                left join dim.user_act_main t3
                  on (t1.fbpid = t3.fbpid and t2.fuid = t3.fuid)
               where t3.dt >= to_date(t1.fbegin_time)
                 and t3.dt < date_add(t1.fbegin_time, 7)
                 and date_add(t1.fbegin_time, 7) >= '%(ld_1day_ago)s'
                 and t1.ftoken_id = 0
                 and t1.fstatus = 3)

            insert overwrite table bud_dm.bud_push_after_info partition (dt='%(ld_1day_ago)s')
            select v1.push_date fdate, v1.fpush_id,
            count(distinct(case when v1.dt = v1.push_date then v1.fuid else null end)) as act_unum_day1,
            count(distinct(case when v1.dt = date_add(v1.push_date,1) then v1.fuid else null end)) as act_unum_day2,
            count(distinct(case when v1.dt = date_add(v1.push_date,2) then v1.fuid else null end)) as act_unum_day3,
            count(distinct(case when v1.dt = date_add(v1.push_date,3) then v1.fuid else null end)) as act_unum_day4,
            count(distinct(case when v1.dt = date_add(v1.push_date,4) then v1.fuid else null end)) as act_unum_day5,
            count(distinct(case when v1.dt = date_add(v1.push_date,5) then v1.fuid else null end)) as act_unum_day6,
            count(distinct(case when v1.dt = date_add(v1.push_date,6) then v1.fuid else null end)) as act_unum_day7,
            count(distinct(case when v1.dt = v1.push_date and v1.fparty_num <> 0 then v1.fuid else null end))  as play_unum_day1,
            count(distinct(case when v1.dt = date_add(v1.push_date,1) and v1.fparty_num <> 0 then v1.fuid else null end)) as play_unum_day2,
            count(distinct(case when v1.dt = date_add(v1.push_date,2) and v1.fparty_num <> 0 then v1.fuid else null end)) as play_unum_day3,
            count(distinct(case when v1.dt = date_add(v1.push_date,3) and v1.fparty_num <> 0 then v1.fuid else null end)) as play_unum_day4,
            count(distinct(case when v1.dt = date_add(v1.push_date,4) and v1.fparty_num <> 0 then v1.fuid else null end)) as play_unum_day5,
            count(distinct(case when v1.dt = date_add(v1.push_date,5) and v1.fparty_num <> 0 then v1.fuid else null end)) as play_unum_day6,
            count(distinct(case when v1.dt = date_add(v1.push_date,6) and v1.fparty_num <> 0 then v1.fuid else null end)) as play_unum_day7,
            count(distinct v1.fuid) as act_unum_all,
            count(distinct(case when v1.fparty_num <> 0 then v1.fuid else null end)) as play_unum_all
            from active_user_num v1
            group by v1.push_date,v1.fpush_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            with party_num as
             (select t1.fpush_id,
                     t2.fuid,
                     t3.fsubname,
                     to_date(t1.fbegin_time) as push_date,
                     t3.dt as dt,
                     sum(t3.fparty_num) fparty_num,
                     t4.fgamefsk
                from stage.push_config_stg t1
               inner join dim.user_push_report t2
                  on t1.fpush_id = t2.fpush_id
                 and t2.faction = 1
                left join dim.user_gameparty t3
                  on (t1.fbpid = t3.fbpid and t2.fuid = t3.fuid)
                join dim.bpid_map t4
                  on t1.fbpid = t4.fbpid
               where t3.dt >= to_date(t1.fbegin_time)
                 and t3.dt < date_add(t1.fbegin_time, 7)
                 and date_add(t1.fbegin_time, 7) >= '%(ld_1day_ago)s'
                 and t1.ftoken_id = 0
                 and t1.fstatus = 3
               group by t1.fpush_id,
                        t2.fuid,
                        t3.fsubname,
                        to_date(t1.fbegin_time),
                        t3.dt,
                        t4.fgamefsk)
            insert overwrite table bud_dm.bud_push_after_gameparty_detail partition (dt='%(ld_1day_ago)s')
            select v2.push_date fdate,
            v2.fgamefsk,
            v2.fpush_id,
            v2.fsubname as fsubname,
            count(distinct(case when v2.dt = v2.push_date and v2.fparty_num >0 then v2.fuid else null end)) as play_unum_day1,
            count(distinct(case when v2.dt = date_add(v2.push_date,1) and v2.fparty_num >0 then v2.fuid else null end)) as play_unum_day2,
            count(distinct(case when v2.dt = date_add(v2.push_date,2) and v2.fparty_num >0 then v2.fuid else null end)) as play_unum_day3,
            count(distinct(case when v2.dt = date_add(v2.push_date,3) and v2.fparty_num >0 then v2.fuid else null end)) as play_unum_day4,
            count(distinct(case when v2.dt = date_add(v2.push_date,4) and v2.fparty_num >0 then v2.fuid else null end)) as play_unum_day5,
            count(distinct(case when v2.dt = date_add(v2.push_date,5) and v2.fparty_num >0 then v2.fuid else null end)) as play_unum_day6,
            count(distinct(case when v2.dt = date_add(v2.push_date,6) and v2.fparty_num >0 then v2.fuid else null end)) as play_unum_day7,
            count(distinct(case when v2.dt = push_date and v2.fparty_num >0  then v2.fparty_num else null end)) as partynum_day1,
            count(distinct(case when v2.dt = date_add(v2.push_date,1) and v2.fparty_num >0 then v2.fparty_num else null end)) as partynum_day2,
            count(distinct(case when v2.dt = date_add(v2.push_date,2) and v2.fparty_num >0 then v2.fparty_num else null end)) as partynum_day3,
            count(distinct(case when v2.dt = date_add(v2.push_date,3) and v2.fparty_num >0 then v2.fparty_num else null end)) as partynum_day4,
            count(distinct(case when v2.dt = date_add(v2.push_date,4) and v2.fparty_num >0 then v2.fparty_num else null end)) as partynum_day5,
            count(distinct(case when v2.dt = date_add(v2.push_date,5) and v2.fparty_num >0 then v2.fparty_num else null end)) as partynum_day6,
            count(distinct(case when v2.dt = date_add(v2.push_date,6) and v2.fparty_num >0 then v2.fparty_num else null end)) as partynum_day7
            from party_num v2
            group by v2.push_date,v2.fpush_id,v2.fsubname,v2.fgamefsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 生成Hive统计实例
a = agg_bud_push_user_after_info(sys.argv[1:])
a()
