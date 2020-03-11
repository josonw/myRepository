#! /usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os, os.path
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc


"""
存放Hive SQL常量的文件
"""

"""
多维度去重实现的SQL
"""
HQL_GROUP_BY_ALL = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id),"
            "(%(bpid_tbl_alias)sfgamefsk,%(src_tbl_alias)sfgame_id))")

HQL_GROUP_BY_FUID_ALL = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(src_tbl_alias)sfgame_id,%(src_tbl_alias)sfuid))")

# 已废弃，不用
HQL_GROUP_BY_FUID_NO_SUB_GAME = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid), "
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid#! /usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os, os.path
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc


"""
存放Hive SQL常量的文件
"""

"""
多维度去重实现的SQL
"""
HQL_GROUP_BY_ALL = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code), "
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id), (%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfgame_id),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfchannel_code),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfterminaltypefsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk))")

HQL_GROUP_BY_FUID_ALL = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid), "
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(src_tbl_alias)sfuid), (%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfgame_id,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfuid))")

HQL_GROUP_BY_FUID_NO_SUB_GAME = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid), "
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfuid), (%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfchannel_code,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfuid))")

"""
多维度去重实现的SQL,含子游戏的去重组合
"""

HQL_GROUP_BY_INCLUDE_SUB_GAME = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code), "
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id), (%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfgame_id))")


"""
多维度去重实现的SQL,去重组合不包含子游戏
"""

HQL_GROUP_BY_NO_SUB_GAME = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code "
            "grouping sets ("
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfchannel_code),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfterminaltypefsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk))")
#################################################################################################################
"""
根据去重方案，参考http://wss.oa.com/default_task_edit.php?pagetab=alltask&editID=779
为各种去重编号
按链接表格行号编号
"""
HQL_CONVERT_GROUPID_ALL = """case
                  when GROUPING__ID=127 then 1
                  when GROUPING__ID=15 then 2
                  when GROUPING__ID=11 then 3
                  when GROUPING__ID=119 then 4
                  when GROUPING__ID=55 then 5
                  when GROUPING__ID=23 then 6
                  when GROUPING__ID=7 then 7
                  when GROUPING__ID=19 then 8
                  when GROUPING__ID=3 then 9
                 else 0 end"""


HQL_CONVERT_GROUPID_NO_SUB_GAME = """case
                  when GROUPING__ID=63 then 4
                  when GROUPING__ID=31 then 5
                  when GROUPING__ID=15 then 6
                  when GROUPING__ID=7 then 7
                  when GROUPING__ID=11 then 8
                  when GROUPING__ID=3 then 9
                 else 0 end"""


"""去重策略引起字符串字段为空时的默认值"""
NULL_STR_GROUP_RULE = "-21379"

"""去重策略引起INT类型字段为空时的默认值"""
NULL_INT_GROUP_RULE = -21379

"""业务未上报数据引起字符串字段为空的默认值"""
NULL_STR_REPORT = "-13658"

"""业务未上报数据引起INT类型字段为空的默认值"""
NULL_INT_REPORT = -13658


GROUPSET = {'alias':['bpid_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias'],
              'field':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code'],
             'comb_value':[[1,  1,  1,  1,  1,  1,  1],
                           [1,  1,  1,  1,  0,  0,  0],
                           [1,  1,  0,  1,  0,  0,  0],
                           [1,  1,  1,  0,  1,  1,  1],
                           [1,  1,  1,  0,  1,  1,  0],
                           [1,  1,  1,  0,  1,  0,  0],
                           [1,  1,  1,  0,  0,  0,  0],
                           [1,  1,  0,  0,  1,  0,  0],
                           [1,  1,  0,  0,  0,  0,  0]]}


GROUPSET_FUID = {'alias':['bpid_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias','src_tbl_alias'],
                  'field':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code','fuid'],
                  'comb_value':[[1,  1,  1,  1,  1,  1,  1,  1],
                               [1,  1,  1,  1,  0,  0,  0,  1],
                               [1,  1,  0,  1,  0,  0,  0,  1],
                               [1,  1,  1,  0,  1,  1,  1,  1],
                               [1,  1,  1,  0,  1,  1,  0,  1],
                               [1,  1,  1,  0,  1,  0,  0,  1],
                               [1,  1,  1,  0,  0,  0,  0,  1],
                               [1,  1,  0,  0,  1,  0,  0,  1],
                               [1,  1,  0,  0,  0,  0,  0,  1]]}


GROUPSET_NO_HALLMODE = {'alias':['bpid_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias'],
              'field':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code'],
              'comb_value':[[1,  1,  0,  0,  1,  1,  1],
                            [1,  1,  0,  0,  1,  1,  0],
                            [1,  1,  0,  0,  1,  0,  0],
                            [1,  1,  0,  0,  0,  0,  0]]}




def extend_groupset(GROUPSET1, GROUPSET2 = None, GROUPSET3 = None ):

    if not GROUPSET2:
        GROUPSET2 = {'alias':[],'field':[],'comb_value':[ [] ] }

    if not GROUPSET3:
        GROUPSET3 = {'alias':[],'field':[],'comb_value':[ [] ] }

    GROUPSET1['alias'].extend(GROUPSET2['alias'])
    GROUPSET1['field'].extend(GROUPSET2['field'])

    GROUPSET1['alias'].extend(GROUPSET3['alias'])
    GROUPSET1['field'].extend(GROUPSET3['field'])

    GROUPBY="group by " + ','.join([ '%({0})s{1}'.format(v,GROUPSET1['field'][i] ) for i,v in enumerate(GROUPSET1['alias'])])

    tmp = []
    for combvalue in GROUPSET1['comb_value']:
        for combvalue2 in GROUPSET2['comb_value']:
            for combvalue3 in GROUPSET3['comb_value']:
                temp = []
                temp.extend(combvalue)
                temp.extend(combvalue2)
                temp.extend(combvalue3)
                #
                groupstr = ','.join( ['%({0})s{1}'.format(GROUPSET1['alias'][i],GROUPSET1['field'][i]) for i,j in enumerate(temp) if j == 1] )
                tmp.append(groupstr)

    GROUPBYALL = '%s%s%s%s' %(GROUPBY , '\ngrouping sets (\n(' , '),\n('.join(tmp) , ')\n)')
    return GROUPBYALL


def multi_div_select_str(isreport=1):
    """isreport=1   select 在一般 group 时 七个维度
       isreport=0   select 在grouping sets 时 七个维度
       isreport=-1  select 在grouping sets 时 重命名后的七个维度（不带表名）
       isreport=-2  select 在一般 group 时 七个维度（带表名）
    """
    if isreport == -1:
        return """fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode"""

    elif isreport == -2:
        return """b.fgamefsk, b.fplatformfsk, b.fhallfsk, a.fgame_id, b.fterminaltypefsk, b.fversionfsk, a.fchannel_code"""

    elif isreport == 1:
        null_rule = '%(null_int_report)d'

    else:
        null_rule = '%(null_int_group_rule)d'

    select_str = """
    %(bpid_tbl_alias)sfgamefsk,
    %(bpid_tbl_alias)sfplatformfsk,
    coalesce(%(bpid_tbl_alias)sfhallfsk,{0}) fhallfsk,
    coalesce(%(src_tbl_alias)sfgame_id,{1}) fsubgamefsk,
    coalesce(%(bpid_tbl_alias)sfterminaltypefsk,{2}) fterminaltypefsk,
    coalesce(%(bpid_tbl_alias)sfversionfsk,{3}) fversionfsk,
    coalesce(%(src_tbl_alias)sfchannel_code,{4}) fchannelcode""".format(null_rule,null_rule,null_rule,null_rule,null_rule)

    return select_str

# print multi_div_select_str(0,0)

def const_dict():
    """返回出现空字符串时的默认值"""
    sql_const_dict = {'null_str_group_rule':NULL_STR_GROUP_RULE,
                      'null_int_group_rule':NULL_INT_GROUP_RULE,
                      'null_str_report':NULL_STR_REPORT,
                      'null_int_report':NULL_INT_REPORT
                      }

    return sql_const_dict


def query_list(stat_date, alias_dic, EXTEND_GROUPSET1, ALL_GROUP = None, NOHALLMODE_GROUP = None):
    """返回hql指定参数值的列表"""
    if not ALL_GROUP:
        ALL_GROUP = GROUPSET
    if not NOHALLMODE_GROUP:
        NOHALLMODE_GROUP = GROUPSET_NO_HALLMODE

    temp = dict(alias_dic,**const_dict())

    query_common ={ 'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],
                    'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
                    'select_field_str':multi_div_select_str(isreport=0)%temp,
                    'select_subquery':multi_div_select_str(isreport=-2)
                   }

    # query[0] 含大厅模式的游戏分组，query[1] 非大厅模式的游戏分组
    query =[{'group_by':extend_groupset(ALL_GROUP, EXTEND_GROUPSET1) % alias_dic,
             'hallmode':1},
            {'group_by':extend_groupset(NOHALLMODE_GROUP, EXTEND_GROUPSET1) % alias_dic,
             'hallmode':0}]

    for i in range(2):
        query[i].update(query_common)
        query[i].update(const_dict())
        query[i].update(PublicFunc.date_define(stat_date))

    return query

_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(src_tbl_alias)sfuid),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfuid))")

"""
多维度去重实现的SQL,含子游戏的去重组合
"""
# 已废弃，不用
HQL_GROUP_BY_INCLUDE_SUB_GAME = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code "
            "grouping sets ((%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code), "
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(src_tbl_alias)sfgame_id),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(src_tbl_alias)sfgame_id))")


"""
多维度去重实现的SQL,去重组合不包含子游戏
"""
# 已废弃，不用
HQL_GROUP_BY_NO_SUB_GAME = ("group by %(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,"
            "%(src_tbl_alias)sfchannel_code "
            "grouping sets ("
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk,%(src_tbl_alias)sfchannel_code),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk,%(bpid_tbl_alias)sfversionfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk,%(bpid_tbl_alias)sfterminaltypefsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfhallfsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk,%(bpid_tbl_alias)sfterminaltypefsk),"
            "(%(bpid_tbl_alias)sfgamefsk,%(bpid_tbl_alias)sfplatformfsk))")
#################################################################################################################
"""
根据去重方案，参考http://wss.oa.com/default_task_edit.php?pagetab=alltask&editID=779
为各种去重编号
按链接表格行号编号
"""
HQL_CONVERT_GROUPID_ALL = """case
                  when GROUPING__ID=127 then 1
                  when GROUPING__ID=15 then 2
                  when GROUPING__ID=11 then 3
                  when GROUPING__ID=119 then 4
                  when GROUPING__ID=55 then 5
                  when GROUPING__ID=23 then 6
                  when GROUPING__ID=7 then 7
                  when GROUPING__ID=19 then 8
                  when GROUPING__ID=3 then 9
                 else 0 end"""


HQL_CONVERT_GROUPID_NO_SUB_GAME = """case
                  when GROUPING__ID=63 then 4
                  when GROUPING__ID=31 then 5
                  when GROUPING__ID=15 then 6
                  when GROUPING__ID=7 then 7
                  when GROUPING__ID=11 then 8
                  when GROUPING__ID=3 then 9
                 else 0 end"""


"""去重策略引起字符串字段为空时的默认值"""
NULL_STR_GROUP_RULE = "-21379"

"""去重策略引起INT类型字段为空时的默认值"""
NULL_INT_GROUP_RULE = -21379

"""业务未上报数据引起字符串字段为空的默认值"""
NULL_STR_REPORT = "-13658"

"""业务未上报数据引起INT类型字段为空的默认值"""
NULL_INT_REPORT = -13658


GROUPSET = {'alias':['bpid_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias'],
              'field':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code'],
             'comb_value':[[1,  1,  1,  1,  1,  1,  0],
                           [1,  1,  1,  0,  0,  0,  0],
                           [1,  1,  1,  0,  1,  1,  0],
                           [1,  1,  1,  1,  0,  0,  0],
                           [1,  0,  0,  1,  0,  0,  0]]}


GROUPSET_FUID = {'alias':['bpid_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias','src_tbl_alias'],
                  'field':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code','fuid'],
                  'comb_value':[[1,  1,  1,  1,  1,  1,  0, 1],
                                [1,  1,  1,  0,  0,  0,  0, 1],
                                [1,  1,  1,  0,  1,  1,  0, 1],
                                [1,  1,  1,  1,  0,  0,  0, 1],
                                [1,  0,  0,  1,  0,  0,  0, 1]]}


GROUPSET_NO_HALLMODE = {'alias':['bpid_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias', 'bpid_tbl_alias','bpid_tbl_alias','src_tbl_alias'],
              'field':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code'],
              'comb_value':[[1,  1,  0,  0,  1,  1,  0],
                            [1,  1,  0,  0,  0,  0,  0]]}




def extend_groupset(GROUPSET1, GROUPSET2 = None, GROUPSET3 = None ):

    if not GROUPSET2:
        GROUPSET2 = {'alias':[],'field':[],'comb_value':[ [] ] }

    if not GROUPSET3:
        GROUPSET3 = {'alias':[],'field':[],'comb_value':[ [] ] }

    GROUPSET1['alias'].extend(GROUPSET2['alias'])
    GROUPSET1['field'].extend(GROUPSET2['field'])

    GROUPSET1['alias'].extend(GROUPSET3['alias'])
    GROUPSET1['field'].extend(GROUPSET3['field'])

    GROUPBY="group by " + ','.join([ '%({0})s{1}'.format(v,GROUPSET1['field'][i] ) for i,v in enumerate(GROUPSET1['alias'])])

    tmp = []
    for combvalue in GROUPSET1['comb_value']:
        for combvalue2 in GROUPSET2['comb_value']:
            for combvalue3 in GROUPSET3['comb_value']:
                temp = []
                temp.extend(combvalue)
                temp.extend(combvalue2)
                temp.extend(combvalue3)
                #
                groupstr = ','.join( ['%({0})s{1}'.format(GROUPSET1['alias'][i],GROUPSET1['field'][i]) for i,j in enumerate(temp) if j == 1] )
                tmp.append(groupstr)

    GROUPBYALL = '%s%s%s%s' %(GROUPBY , '\ngrouping sets (\n(' , '),\n('.join(tmp) , ')\n)')
    return GROUPBYALL


def multi_div_select_str(isreport=1):
    """isreport=1   select 在一般 group 时 七个维度
       isreport=0   select 在grouping sets 时 七个维度
       isreport=-1  select 在grouping sets 时 重命名后的七个维度（不带表名）
       isreport=-2  select 在一般 group 时 七个维度（带表名）
    """
    if isreport == -1:
        return """fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode"""

    elif isreport == -2:
        return """b.fgamefsk, b.fplatformfsk, b.fhallfsk, a.fgame_id, b.fterminaltypefsk, b.fversionfsk, a.fchannel_code"""

    elif isreport == 1:
        null_rule = '%(null_int_report)d'

    else:
        null_rule = '%(null_int_group_rule)d'

    select_str = """
    %(bpid_tbl_alias)sfgamefsk,
    %(bpid_tbl_alias)sfplatformfsk,
    coalesce(%(bpid_tbl_alias)sfhallfsk,{0}) fhallfsk,
    coalesce(%(src_tbl_alias)sfgame_id,{1}) fsubgamefsk,
    coalesce(%(bpid_tbl_alias)sfterminaltypefsk,{2}) fterminaltypefsk,
    coalesce(%(bpid_tbl_alias)sfversionfsk,{3}) fversionfsk,
    coalesce(%(src_tbl_alias)sfchannel_code,{4}) fchannelcode""".format(null_rule,null_rule,null_rule,null_rule,null_rule)

    return select_str

# print multi_div_select_str(0,0)

def const_dict():
    """返回出现空字符串时的默认值"""
    sql_const_dict = {'null_str_group_rule':NULL_STR_GROUP_RULE,
                      'null_int_group_rule':NULL_INT_GROUP_RULE,
                      'null_str_report':NULL_STR_REPORT,
                      'null_int_report':NULL_INT_REPORT
                      }

    return sql_const_dict


def query_list(stat_date, alias_dic, EXTEND_GROUPSET1, ALL_GROUP = None, NOHALLMODE_GROUP = None):
    """返回hql指定参数值的列表"""
    if not ALL_GROUP:
        ALL_GROUP = GROUPSET
    if not NOHALLMODE_GROUP:
        NOHALLMODE_GROUP = GROUPSET_NO_HALLMODE

    temp = dict(alias_dic,**const_dict())

    query_common ={ 'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],
                    'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
                    'select_field_str':multi_div_select_str(isreport=0)%temp,
                    'select_subquery':multi_div_select_str(isreport=-2)
                   }

    # query[0] 含大厅模式的游戏分组，query[1] 非大厅模式的游戏分组
    query =[{'group_by':extend_groupset(ALL_GROUP, EXTEND_GROUPSET1) % alias_dic,
             'hallmode':1},
            {'group_by':extend_groupset(NOHALLMODE_GROUP, EXTEND_GROUPSET1) % alias_dic,
             'hallmode':0}]

    for i in range(2):
        query[i].update(query_common)
        query[i].update(const_dict())
        query[i].update(PublicFunc.date_define(stat_date))

    return query

