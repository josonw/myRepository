#-*- coding: UTF-8 -*-
import os
import sys
import re

output = open('output.txt', 'w')

def job_info_field():
    """调度系统任务信息字段
    """
    return {
        'jid':0,            #'任务ID',
        'pjid':0,            #'父任务ID集',
        'pri':3,            #'优先级',
        'jobname':0,        #'任务名称',
        'jobdesc':'',        #'任务详细描述',
        'jobtype':'计算任务',        #'任务类型，入库任务、计算任务、结果数据同步任务',
        'tab_from':0,        #'任务查询关联表，多表可用逗号分隔',
        'tab_into':0,        #'任务结果存储表，多表可用逗号分隔',
        'calling':0,        #'脚本路径名',
        'jobcycle':'d',        #'任务周期类型，小时、日、周、月',
        'maxtime':3600,        #'超时阀值(秒)',
        'maxrerun':3,        #'重启次数',
        'regap':60,            #'重启缓冲(秒)',
        'alarm':4,            #'1：关闭告警；2：邮件告警；3：RTX告警；4：短信告警；5：短信群发；6：邮件+RTX告警',
        'update_time':0,    #'更新时间',
        'open':1,            #'0废弃1开启',
    }


def grep_form_into_table_name( file_path ):
    """正则匹配，insert，from table信息
    # 主要找出表之间依赖关系
    """
    pattern = re.compile(r'\s.*(from|join|table|exists)\s*([a-z._]*)\s.*',re.I)
    into_tab, form_tab = [], []
    for line in open(file_path):
        match = pattern.match( line )
        if match:
            ctype, table = match.groups()
            ctype, table = ctype.lower(), table.lower()
            if 'tmp' not in table:
                if ctype in ('exists', 'table' ):
                    if table not in into_tab and table not in ('if'):
                        into_tab.append(table)
                if ctype in ('from', 'join'):
                    if table not in form_tab:
                        form_tab.append(table)
    into_tab = [ t for t in into_tab if t not in form_tab ]

    return {
        'into_tab':into_tab,
        'form_tab':form_tab,
    }

def main(path_url):
    try:
        listfile=os.listdir(path_url)
    except Exception, e:
        output.write('oops! your url path was wrong, please check.\n')
        return
    job_list = []
    for py in listfile:
        job_info = job_info_field()
        job_info['jobname'] = py.split('.py')[0]
        file_path = os.path.join( path_url, py)
        job_info['calling'] = file_path.split('jobs\\')[1].replace("\\","/")
        tables = grep_form_into_table_name( file_path )
        job_info['tab_into'] = ','.join( tables['into_tab'] )
        job_info['tab_from'] = ','.join( tables['form_tab'] )
        job_list.append(job_info)

    keys = ['pjid','pri','jobname','jobdesc','jobtype','tab_from','tab_into','calling','jobcycle','maxtime','maxrerun','regap','alarm','update_time','open']
    for job_info in job_list:
        txt = "(%s)," % ( ','.join( [ "'%s'"% job_info.get(k, '') for k in keys] ) )
        output.write(txt+'\n')

insert_sql = """ INSERT INTO `job_config` (`pjid`, `pri`, `jobname`, `jobdesc`, `jobtype`, `tab_from`,
        `tab_into`, `calling`, `jobcycle`, `maxtime`, `maxrerun`, `regap`, `alarm`, `update_time` , `open`)
        VALUES """
job_path = 'D:\\data_oa\\byl_scs\jobs\stat\channel'

output.write(insert_sql + '\n')
main(job_path)
output.close()
