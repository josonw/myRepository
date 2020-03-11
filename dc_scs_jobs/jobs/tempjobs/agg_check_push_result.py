# -*- coding: utf-8 -*-
'''

'''
import os
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
from dc_scs_jobs import config
from libs.DB_Mysql import Connection as m_db
import time
import paramiko


class ShellExec(object):

    host = '10.30.101.94'
    port = 3600
    user = 'hadoop'
    pkey = '/home/hadoop/.ssh/id_rsa'

    def __init__(self):
        self._connect()

    def _connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.load_system_host_keys()
        self.ssh.connect(hostname=self.host, port=self.port, username=self.user, key_filename=self.pkey)

    def execute(self, cmd, timeout):
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout)
        return stdout.read(), stderr.read()

class CheckPushResult(object):


    def __init__(self):

        self.mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )


    def result_callback_hql(self):
        # push_num:今天的推送次数
        # success_num:今天推送成功次数
        # click_num:今天推送的点击次数
        # appear_num:今天推送的展示次数
        today1h_ago = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(time.time())-3600))
        today_now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(time.time())))
        ids = self.mdb.query("""select distinct jobname as push_id
                             from (select jobname,
                                          from_unixtime(max(substring(substring_index(tempsql,'begin_time',-1), 8, 10) + 0)) begin_time,
                                          from_unixtime(max(substring(substring_index(tempsql,'end_time',-1), 8, 10) + 0)) end_time
                                     from tjob_detail t
                                    where jobtype='pushjobs'
                                      and substring(substring_index(tempsql,'fpush_platform',-1), 8, 11) = 'boyaa_umeng'
                                    group by jobname
                                   ) t
                            where end_time >= '%s'
                              and end_time <= '%s' """%(today1h_ago, today_now))
        msgids = ','.join(["'%s'"%i.get("push_id") for i in ids])
        pushids = ','.join([str("'"+i.get("push_id")+"'") for i in ids])

        print 'time1h_ago={}'.format(today1h_ago)
        print 'time_now={}'.format(today_now)
        print 'msgids={}'.format(msgids)
        print 'pushids={}'.format(pushids)


        if not msgids or not pushids:
            print 'msgids={}, pushids={}, auto return'.format(msgids, pushids)
            return

        insert_hql = """
            insert overwrite table pushdb.push_user_second
            partition(fpushid)
            select distinct t.fuid, t.fpush_platform ftoken, 'umeng' fpush_platform, t.fpushid
              from pushdb.push_user t
              left join (select t.fpushid, t.fuid
                           from pushdb.push_user t
                           join (select distinct fmsgid, fclient_id
                                   from stage.user_push_report_stg
                                  where fmsgid in ({msgids})
                                    and faction = 1
                                ) t1
                             on t.fpushid = t1.fmsgid
                            and t.ftoken = t1.fclient_id
                          where t.fpushid in ({pushids})
                        ) t2
                on t.fpushid = t2.fpushid
               and t.fuid = t2.fuid
             where t.fpushid in ({pushids})
               and t2.fuid is null
               """.format(msgids=msgids, pushids=pushids)

        cmds = []
        data = {}
        data['mapreduce_cnt'] = "set mapreduce.job.queuename=tempjobs"
        data['tempsql'] = insert_hql
        execmd1 = """source ~/.bash_profile; hive -e "%(mapreduce_cnt)s; %(tempsql)s" """ % data
        execmd1 = execmd1.encode('utf-8')    #执行统计数据，并插入表里面
        cmds.append({'cmd':execmd1,'timeout':2500 })

        for pushid in [item.replace("'", '')for item in pushids.split(',')]:              #每一个pushid产生一个文件
            sql = "select distinct ftoken from pushdb.push_user_second t where fpushid='{pushid}' ".format(pushid=pushid)
            data['jobname'] = str(pushid)+'_second'
            data['dir'] = 'push_data_dev' if data['jobname'].find('dev')>=0 else 'push_data'
            data['tempsql'] = sql          #将表里面的数据取出，分类插入到文件
            data['mapreduce_cnt'] = "%s;%s"%(data['mapreduce_cnt'],"set hive.cli.print.header=false")

            cmd1 = """source ~/.bash_profile; hive -e "%(mapreduce_cnt)s; %(tempsql)s" > /data/workspace/push_mid/%(jobname)s.csv;""" % data
            cmd = {'cmd':cmd1.encode('utf-8'), 'timeout':2500}
            cmds.append(cmd)  #从hive中导出
            cmd2 = "cd /data/workspace/push_mid/; sed -i 's/\\t/,/g' %(jobname)s.csv; gzip -f %(jobname)s.csv; cp %(jobname)s.csv.gz /data/workspace/%(dir)s/ " % data
            cmd = {'cmd':cmd2.encode('utf-8'), 'timeout':200}  #拷贝到对应目录
            cmds.append(cmd)

        s = ShellExec()
        for item in cmds:
            print item['cmd']
            out, err = s.execute(item['cmd'], item['timeout'])
            print out, err








if __name__ == '__main__':

    obj = CheckPushResult()
    obj.result_callback_hql()

