#-*- coding: UTF-8 -*-
import os
import sys
import datetime
import time
#把 str 编码由 ascii 改为 utf8 (或 gb18030)
reload(sys)
sys.setdefaultencoding( "utf-8" )

dc_path = os.getenv('DC_SERVER_PATH')
from sys import path
path.append( dc_path )

from dc_scs_jobs import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms

def get_members(mdb,is_time,sort_item = None):
    """ 找出相应配置的人员，并排好序"""
    sql = 'SELECT * FROM loop_member where isworktime >= %s' %is_time
    data = mdb.query(sql)
    if sort_item == None:
        sort_item = 'priority'
    temp = sorted(data, key = lambda x:x[sort_item], reverse=False)
    result = [item['user'] for item in temp]
    return result

def is_worktime():
    """ 判断当前时间是否是工作时间 """
    h = int(time.strftime("%H", time.localtime()))
    # d = int(time.strftime("%w", time.localtime()))
    if 9<=h<=24:
        return 1
    else:
        return 0

def checkJobStart(mdb, str_day):
    """ 检查pre_all脚本有没有成功运行完成 """
    sql_dict = {'fname':u'调度任务启动', 'fstatus':1, 'fmsg':u'调度任务检查失败'}

    sql = "SELECT * FROM `job_entity` WHERE jid = 1000 AND d_time = '%s'" % str_day
    data = mdb.query(sql)
    if 0==len(data): #实例化都没有
        sql_dict.update({'fmsg':u'调度任务没有实例化'})
    elif 6 != int(data[0]['status']):
        sql_dict.update({'fmsg':u'调度任务没有开始执行'})
    else:
        sql_dict.update({'fstatus':0, 'fmsg':u'调度任务启动->正常'})

    in_sql = """INSERT INTO `dserver_status`
        SET `ftime`=unix_timestamp(now()), `fname`='%(fname)s',
        `fstatus`=%(fstatus)s, `fmsg`='%(fmsg)s'""" % sql_dict
    print in_sql
    mdb.execute(in_sql)


def checkthreshold(process, h, m):
    """ 总进度检查函数
        5-7点之间，总进度低于最小值打电话，大于最大值不发短信，之间发短信。其他时间短信通知负责人
    """
    threshold_dic = { '05:00' : {'min':5, 'max':30 },
                      '05:30' : {'min':15, 'max':35 },
                      '06:00' : {'min':20, 'max':90 },
                      '06:30' : {'min':25, 'max':90 },
                      '07:00' : {'min':30, 'max':90 },
                      '07:30' : {'min':35, 'max':90 }
                    }
    # threshold_dic = { '05:00' : {'min':20, 'max':45 },
    #                   '05:30' : {'min':30, 'max':50 },
    #                   '06:00' : {'min':35, 'max':55 },
    #                   '06:30' : {'min':40, 'max':65 },
    #                   '07:00' : {'min':45, 'max':70 },
    #                   '07:30' : {'min':50, 'max':75 }
    #                 }
    if 5 <= int(h) <= 7 :
        if int(m) > 29:
            htime =  h + ':30'
        else:
            htime =  h + ':00'
        if process > threshold_dic[htime]['max']:
            return 0
        elif threshold_dic[htime]['min']<= process<= threshold_dic[htime]['max']:
            return 3
        else:
            return 8
    return 3

def checkJobsRunning(mdb, str_day, hour, minute):
    """ 反向检查调度完成情况，30分钟检查一次 """
    sql_dict = {'fname':u'调度任务检查', 'fstatus':0, 'fmsg':u'调度任务检查正常'}

    today = time.strftime('%Y-%m-%d')
    data = mdb.query("SELECT d_time, ftotle, fpending, fready, frunnig,fouttime, fother, ferror, fstay, ferrmsg, fdone, fnum FROM `job_status` ORDER BY id DESC LIMIT 2")
    keys =  ['d_time', 'ftotle', 'fpending', 'fready', 'frunnig','fouttime', 'fother', 'ferror', 'fstay', 'ferrmsg', 'fdone']
    info = getRunnigStatus(mdb, str_day)     #这个必须在上面SQL运行后执行
    temp1 = (data[0][key] for key in keys)
    temp2 = (data[1][key] for key in keys)
    temp3 = (info[key] for key in keys)
    tmp = data[0]['d_time'].split(' ')
    # 两类成员顺序，电话告警时优先拨打第一位的
    isworktime = is_worktime()
    if today == str(tmp[0]):
        if temp1 == temp2 == temp3:   ##半个小时后任务状态还一样
            if (info['fpending'] + info['fready'] + info['ferror']) > 10:        #确认不是全部执行完成
                sql_dict.update({'fstatus':1, 'fmsg':u'任务累计超过10个且半个小时没有新任务完成'})
                send_sms(get_members(mdb, isworktime), sql_dict['fmsg'], 8)    #临时加的德州客服电话告警

        if info.get('ferrmsg'):
            if  info['ferrmsg'].count(u'分区') > 0:         #如果大于0，说明分区任务出现了错误,按数据传输顺序发送短信
                sql_dict.update({'fstatus':1, 'fmsg':u'分区任务出现错误'})
                sort_item = 'uid'
                send_sms(get_members(mdb, isworktime,sort_item), sql_dict['fmsg'], 8)

        # 没有出现以上两种情况，并且没有剩余任务(只有停止任务和待启动任务将不短信通知)，没有出现错误信息了，则不发送短信。
        if sql_dict['fstatus']!=1:
            if not info.get('ftotle') - info.get('fother') - info.get('fstay'):
                if not info.get('ferrmsg'):
                    return True

    in_sql = """INSERT INTO `dserver_status` SET `ftime`=unix_timestamp(now()), `fname`='%(fname)s',
        `fstatus`=%(fstatus)s, `fmsg` = '%(fmsg)s' """ % sql_dict
    print in_sql
    mdb.execute(in_sql)

    info['process'] = info['fdone']*100/info['fnum']

    # warning_msg = """调度系统监测：今日任务总共 %(fnum)s 个，完成 %(done)s 个 剩余 %(ftotle)s 个。
    #     当前 %(fpending)s 个挂起，%(fready)s 个就绪，%(frunnig)s 个运行，%(ferror)s 个出错，%(fstay)s 个待启动""" % info

    warning_msg = u"""调度系统监测：
    进度: %(process)s%% (%(fdone)s/%(fnum)s)
    运行: %(frunnig)s
    剩余: %(ftotle)s
    挂起: %(fpending)s
    就绪: %(fready)s
    超时: %(fouttime)s
    出错: %(ferror)s %(ferrmsg)s""" % info

    threshold = checkthreshold(info['process'], hour, minute)

    if threshold:
        send_sms(get_members(mdb, isworktime), warning_msg, threshold)

    if threshold==8 and hour in ('07','08'):
        # 出现进度太慢时在6,7,8点钟改变调度策略相对来说总进度会最大化
        mdb.execute("update loop_config set config_value='FIFO' where config_name='schedule_policy' ")

    print warning_msg


def getRunnigStatus(mdb, str_day):
    info = {'fpending':  0,
            'fready':    0,
            'frunnig':   0,
            'fouttime':  0,
            'ferror':    0,
            'fstay':     0,
            'fother':    0,
            'ferrmsg':   '' }

    # 任务类型别名，太长了短信中不方便看,短信文本过长可能会被截成两条
    jobtype_dict = {u'hive计算':u'h计算',u'pg计算':u'p计算',
                    u'同步hive':u'同步h',u'同步pg':u'同步p',
                    u'pgc计算':u'c计算',u'同步pgc':u'同步c',
                   u'分区':u'分区',u'spark':u'spark',u'impala':u'impala'}

    errmsg_dict = {v:0 for k,v in jobtype_dict.items()}


    data = mdb.query("SELECT b.jobtype,a.status FROM `job_running` a LEFT JOIN  jconfig b ON a.jid = b.jid")
                                                          #从当前的运行表中取出运行状态
    for row in data:
        status  = row.get('status')

        if 1 == status:
            info['fpending'] += 1
        elif 2 == status:
            info['fready'] += 1
        elif 3 == status:
            info['frunnig'] += 1
        elif 4 == status:
            info['fouttime'] += 1
        elif 5 == status:
            info['ferror'] += 1
            job_type = jobtype_dict.get(row.get('jobtype'),u'未知')
            errmsg_dict[job_type] = errmsg_dict.get(job_type, 0) + 1     #将各类错误类型包装成一个错误字典
        elif 8 == status:
            info['fstay'] += 1
        else:
            info['fother'] += 1

    if info['ferror'] != 0:                   #将错误字典包装成一个字符串
        info['ferrmsg'] = '(' + ','.join( [" %s*%s" % (k, v) for k, v in errmsg_dict.items() if v > 0] ) + ')'

    info['d_time'] = time.strftime('%Y-%m-%d %H:%M')
    fnum = mdb.getOne("""SELECT count(jid) AS fnum FROM jconfig
                        WHERE open = 1 AND (SUBSTRING_INDEX(SUBSTRING_INDEX(jobcycle, ' ', 3),' ', -1) = dayofmonth( now())
                        OR SUBSTRING_INDEX(SUBSTRING_INDEX(jobcycle, ' ', 5),' ', -1) = weekday( now())+1
                        OR (SUBSTRING_INDEX(SUBSTRING_INDEX(jobcycle, ' ', 3),' ', -1) = '*' AND SUBSTRING_INDEX(SUBSTRING_INDEX(jobcycle, ' ', 5),' ', -1) = '*'))
                        """)
    info['fnum'] = int(fnum['fnum'])


    temp = mdb.getOne("""SELECT count(1) as fpending FROM `job_entity` e
                         left join job_running r on e.eid=r.eid
                         WHERE e.status=1 and r.eid is null """)

    info['fpending'] = info['fpending'] + temp['fpending']

    temp = mdb.getOne("""SELECT count(1) as fdone FROM `job_entity` e
                         WHERE e.status=6 and e.d_time= '%s' """ %str_day)

    info['fdone'] = temp['fdone']
    info['fstay'] = info['fnum'] - info['fdone'] - \
                    (info['fpending']+info['fready']+info['frunnig']+info['fouttime']+info['ferror']+info['fother'])

    info['ftotle'] = info['fpending']+info['fready']+info['frunnig']+info['fouttime']+info['ferror']+info['fother']+info['fstay']



    in_sql = """INSERT INTO `job_status` (d_time, ftotle, fpending, fready, frunnig, fouttime,
                                          fother, ferror, fstay, ferrmsg, fdone, fnum)
        VALUES ('%(d_time)s', %(ftotle)s, %(fpending)s, %(fready)s,%(frunnig)s, %(fouttime)s,
                %(fother)s, %(ferror)s, %(fstay)s, '%(ferrmsg)s',%(fdone)s, %(fnum)s)
        """ % info

    print in_sql
    mdb.execute(in_sql)

    return info


if __name__ == '__main__':
    #数据库链接
    mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
    #获取昨天的日期
    str_day = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")

    now = time.localtime()
    h = time.strftime("%H", now)
    m = time.strftime("%M", now)
    if h in ('00', '01', '02', '03'):
        print 'rest'
    elif '04' == h:
        ## 插入运行状态每日第一条信息
        getRunnigStatus(mdb, str_day)

        checkJobStart(mdb, str_day)
    elif int(h)> 18 or int(h)<10:
        checkJobStart(mdb, str_day)
        checkJobsRunning(mdb, str_day, h, m)

    mdb.close
