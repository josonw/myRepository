#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/service' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import sql_const

GROUPSET1 = sql_const.GROUPSET
GROUPSET_FUID = sql_const.GROUPSET_FUID



alias_dic = {'bpid_tbl_alias':'bgm.','src_tbl_alias':'src.','funct':''}


GROUPSET2 = {'alias':['funct', 'src_tbl_alias', 'src_tbl_alias'],
             'field':["case when bgm.fgamefsk = 4132314431 and src.fpname<>'-13658' then '比赛场' else src.fpname end ", 'fname', 'fsubname'],
             'comb_value':[[1,  1,  1],
                          [1,  1,  0],
                          [1,  0,  0],
                          [0,  0,  0]]}


GROUPSET3 = {'alias':['src_tbl_alias','src_tbl_alias','src_tbl_alias','src_tbl_alias', 'src_tbl_alias', 'src_tbl_alias'],
             'field':['fmode', 'ffirst', 'ffirst_sub', 'ffirst_match', 'flag', 'ffirst_gsub'],
             'comb_value':[[1, 1, 1, 1, 1, 1],
                          [0, 1, 1, 1, 1, 1]]}



def casewhenstr():
    newdict = [
    {'k':'fpapplyucnt', 'ffirst_match=':1, 'flag=':1},
    {'k':'fnapplyucnt', 'ffirst=':1, 'flag=':1},
    {'k':'fsapplyucnt', 'ffirst_sub=':1, 'flag=':1},

    {'k':'fppartyucnt', 'ffirst_match=':1, 'flag=':2},
    {'k':'fnpartyucnt', 'ffirst=':1, 'flag=':2},
    {'k':'fspartyucnt', 'ffirst_sub=':1, 'flag=':2},

    {'k':'fpregappucnt',              'flag=':1, 'un.fgamefsk is not null':''},
    {'k':'fnregappucnt', 'ffirst=':1, 'flag=':1, 'un.fgamefsk is not null':''},
    {'k':'fsregappucnt', 'ffirst_sub=':1, 'flag=':1, 'un.fgamefsk is not null':''},

    {'k':'fpregpartyucnt',             'flag=':2, 'un.fgamefsk is not null':''},
    {'k':'fnregpartyucnt', 'ffirst=':1, 'flag=':2, 'un.fgamefsk is not null':''},
    {'k':'fsregpartyucnt', 'ffirst_sub=':1, 'flag=':2, 'un.fgamefsk is not null':''},

    {'k':'fgapplyucnt', 'ffirst_gsub=':1, 'flag=':1},
    {'k':'fgpartyucnt', 'ffirst_gsub=':1, 'flag=':2},
    {'k':'fgregappucnt', 'ffirst_gsub=':1, 'flag=':1, 'un.fgamefsk is not null':''},
    {'k':'fgregpartyucnt', 'ffirst_gsub=':1, 'flag=':2, 'un.fgamefsk is not null':''} ]

    casewhenlist = []
    for field_dict in newdict:
        temp = ' and '.join(['%s%s' %(k,v)for k,v in field_dict.items() if k !='k'])
        temp = 'count(distinct case when {0} then %(src_tbl_alias)sfuid else null end) {1},'.format(temp, field_dict['k'])
        casewhenlist.append(temp)
    casewhenstr = '\n'.join(casewhenlist)
    return casewhenstr


class agg_match_user_fplay(BaseStat):

    def create_tab(self):
        hql = """create table if not exists dcnew.match_user_fplay
            (
            fdate                      date,
            fgamefsk                   bigint,
            fplatformfsk               bigint,
            fhallfsk                   bigint,
            fsubgamefsk                bigint,
            fterminaltypefsk           bigint,
            fversionfsk                bigint,
            fchannelcode               bigint,

            fpname                     varchar(100), --比赛场名称 比如“斗地主比赛场”
            fname                      varchar(100), --二级赛名称 比如“话费赛”
            fsubname                   varchar(50),  --三级赛名称 比如“XX赛(10点-12点)”
            fmode                      varchar(100), --报名模式，比如有偿报名、免费报名

            fpapplyucnt                bigint,  --比赛场新增报名人数
            fnapplyucnt                bigint,  --一级赛新增报名人数
            fsapplyucnt                bigint,  --二级赛新增报名人数

            fppartyucnt                bigint,  --比赛场新增参赛人数
            fnpartyucnt                bigint,  --一级赛新增参赛人数
            fspartyucnt                bigint,  --二级赛新增参赛人数

            fpregappucnt               bigint,  --比赛场新增注册报名人数
            fnregappucnt               bigint,  --一级赛新增注册报名人数
            fsregappucnt               bigint,  --二级赛新增注册报名人数

            fpregpartyucnt             bigint,  --比赛场新增注册参赛人数
            fnregpartyucnt             bigint,  --一级赛新增注册参赛人数
            fsregpartyucnt             bigint,  --二级赛新增注册参赛人数

            fgapplyucnt                bigint,  --三级赛新增报名人数
            fgpartyucnt                bigint,  --三级赛新增参赛人数
            fgregappucnt               bigint,  --三级赛新增注册报名人数
            fgregpartyucnt             bigint   --三级赛新增注册参赛人数
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        hql_list = []
        query = { 'statdate':self.stat_date,"num_date": self.stat_date.replace("-", ""),
            'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],\
            'group_by_fuid':sql_const.extend_groupset(GROUPSET_FUID,GROUPSET2, GROUPSET3) % alias_dic,

            'casewhenstr':casewhenstr()[:-1] % alias_dic,

            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,\
            'null_str_report':sql_const.NULL_STR_REPORT,'null_int_report':sql_const.NULL_INT_REPORT}

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;
                              set hive.groupby.skewindata=true;""")
        if res != 0: return res


        hql = """
        drop table if exists work.match_user_fplay_temp_%(num_date)s;
        create table work.match_user_fplay_temp_%(num_date)s
        as
            select a.fbpid, a.fgame_id, a.fchannel_code, a.fpname, a.fname, a.fsubname,
                  a.fuid,  max(a.fmode) over(partition by a.fmatch_id ,a.fuid) fmode,
                  ffirst, ffirst_sub, ffirst_match,ffirst_gsub, flag
                from
                (
                select jg.fbpid,
                    coalesce(jg.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(jg.fpname,'%(null_str_report)s') fpname,
                    coalesce(jg.fname,'%(null_str_report)s') fname,
                    coalesce(jg.fsubname,'%(null_str_report)s') fsubname,
                    jg.ffirst,jg.ffirst_sub,jg.ffirst_match,ffirst_gsub,jg.fmode,jg.fuid,1 flag, jg.fmatch_id
                from stage.join_gameparty_stg jg
                left join analysis.marketing_channel_pkg_info mcp
                on jg.fchannel_code = mcp.fid where jg.dt='%(statdate)s' and  coalesce(fmatch_id,'0')<>'0'
                group by jg.fbpid,
                    coalesce(jg.fgame_id,cast (0 as bigint)) ,
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(jg.fpname,'%(null_str_report)s'),
                    coalesce(jg.fname,'%(null_str_report)s') ,
                    coalesce(jg.fsubname,'%(null_str_report)s') ,
                    jg.ffirst,jg.ffirst_sub,jg.ffirst_match,ffirst_gsub,jg.fmode,jg.fuid,jg.fmatch_id

                union all

                select ug.fbpid,
                    coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(ug.fpname,'%(null_str_report)s') fpname,
                    coalesce(ug.fsubname,'%(null_str_report)s') fname,
                    coalesce(ug.fgsubname,'%(null_str_report)s') fsubname,
                    ug.ffirst_play ffirst,ug.ffirst_play_sub ffirst_sub,ug.ffirst_match ffirst_match,ffirst_play_gsub ffirst_gsub,
                    null fmode,ug.fuid, 2 flag,ug.fmatch_id
                from stage.user_gameparty_stg ug
                left join analysis.marketing_channel_pkg_info mcp
                on ug.fchannel_code = mcp.fid  where ug.dt='%(statdate)s' and  coalesce(fmatch_id,'0')<>'0'
                group by ug.fbpid,
                    coalesce(ug.fgame_id,cast (0 as bigint)),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(ug.fpname,'%(null_str_report)s'),
                    coalesce(ug.fsubname,'%(null_str_report)s'),
                    coalesce(ug.fgsubname,'%(null_str_report)s'),
                    ug.ffirst_play,ug.ffirst_play_sub,ug.ffirst_match,ffirst_play_gsub,ug.fuid,ug.fmatch_id
                ) a

        """% query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.match_user_fplay
        partition(dt = '%(statdate)s')
        select /*+ MAPJOIN(%(bpid_tbl_alias)s) */ '%(statdate)s' fdate,
                %(src_tbl_alias)s.fgamefsk,
                %(src_tbl_alias)s.fplatformfsk,
                %(src_tbl_alias)s.fhallfsk,
                %(src_tbl_alias)s.fsubgamefsk,
                %(src_tbl_alias)s.fterminaltypefsk,
                %(src_tbl_alias)s.fversionfsk,
                %(src_tbl_alias)s.fchannelcode,

                %(src_tbl_alias)s.fpname,
                %(src_tbl_alias)s.fname,
                %(src_tbl_alias)s.fsubname,
                %(src_tbl_alias)s.fmode,

                %(casewhenstr)s
            from (
                  select
                    %(bpid_tbl_alias)s.fgamefsk,
                    coalesce(%(bpid_tbl_alias)s.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                    coalesce(%(bpid_tbl_alias)s.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                    coalesce(%(src_tbl_alias)s.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                    coalesce(%(bpid_tbl_alias)s.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                    coalesce(%(bpid_tbl_alias)s.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                    coalesce(%(src_tbl_alias)s.fchannel_code,%(null_int_group_rule)d) fchannelcode,

                    coalesce(case when %(bpid_tbl_alias)s.fgamefsk = 4132314431 and %(src_tbl_alias)s.fpname<>'%(null_str_report)s' then '比赛场' else %(src_tbl_alias)s.fpname end,'%(null_str_group_rule)s') fpname,
                    coalesce(%(src_tbl_alias)s.fname,'%(null_str_group_rule)s') fname,
                    coalesce(%(src_tbl_alias)s.fsubname,'%(null_str_group_rule)s') fsubname,
                    coalesce(%(src_tbl_alias)s.fmode,'%(null_str_group_rule)s') fmode,
                    ffirst, ffirst_sub, ffirst_match,ffirst_gsub, fuid, flag
                  from
                    work.match_user_fplay_temp_%(num_date)s %(src_tbl_alias)s
                    join dim.bpid_map %(bpid_tbl_alias)s
                    on %(src_tbl_alias)s.fbpid = %(bpid_tbl_alias)s.fbpid
                    %(group_by_fuid)s
                  ) %(src_tbl_alias)s

           left join ( select fuid, fgamefsk, fplatformfsk, fhallfsk, fgame_id,
                       fterminaltypefsk, fversionfsk, fchannel_code
                       from dim.reg_user_array
                       where dt = '%(statdate)s'
                       ) un
                on %(src_tbl_alias)s.fuid = un.fuid
                and %(src_tbl_alias)s.fgamefsk = un.fgamefsk
                and %(src_tbl_alias)s.fplatformfsk = un.fplatformfsk
                and %(src_tbl_alias)s.fhallfsk = un.fhallfsk
                and %(src_tbl_alias)s.fsubgamefsk = un.fgame_id
                and %(src_tbl_alias)s.fterminaltypefsk = un.fterminaltypefsk
                and %(src_tbl_alias)s.fversionfsk = un.fversionfsk
                and %(src_tbl_alias)s.fchannelcode = un.fchannel_code

        group by %(src_tbl_alias)s.fgamefsk,%(src_tbl_alias)s.fplatformfsk,%(src_tbl_alias)s.fhallfsk,%(src_tbl_alias)s.fsubgamefsk,
                 %(src_tbl_alias)s.fterminaltypefsk,%(src_tbl_alias)s.fversionfsk,%(src_tbl_alias)s.fchannelcode,
                 %(src_tbl_alias)s.fpname,%(src_tbl_alias)s.fname,%(src_tbl_alias)s.fsubname,%(src_tbl_alias)s.fmode
          """% query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        res = self.hq.exe_sql("""drop table work.match_user_fplay_temp_%(num_date)s"""% query)
        if res != 0: return res
        return res


# 愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else:
    args = sys.argv[1].split(',')
    statDate = args[0]



# 生成统计实例
a = agg_match_user_fplay(statDate, eid)
a()
