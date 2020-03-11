#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
import paramiko
import ast
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
# from BaseStatModel import BaseStatModel
from BaseStat import BaseStat
import service.sql_const as sql_const
from libs.DB_Mysql import Connection as m_db
import config

reload(sys)
sys.setdefaultencoding( "utf-8" )

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
        self.ssh.connect(self.host, self.port, self.user, key_filename=self.pkey)

    def execute(self, cmd, timeout):
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=timeout)
        return stdout.read(), stderr.read()


class agg_pushjobs(BaseStat):

    def create_tab(self):
        pass

    def version(self, vfield, selectedValue, inputValue):
        verstr = ""
        verlist = []
        selecteddict={'0':'=', '1':'>', '2':'<', '3':'>=', '4':'<='}

        selectedValue = str(selectedValue).strip()
        selectedValue = selecteddict.get(selectedValue)
        if selectedValue in (">=","<="):
            for i,v in enumerate(inputValue.split(".")):
                vstr="coalesce(regexp_extract(%s,'^([0-9])+\\.([0-9])+\\.([0-9]+)',%s)+0,0)%s%s"%(vfield,i+1,selectedValue,v)
                verlist.append(vstr)
            verstr = " and ".join(verlist)

        elif selectedValue in (">","<"):
            inv = inputValue.split(".")
            for j in range(1,len(inv)+1):
                temp = []
                for i in range(0,j-1):
                    vstr="coalesce(regexp_extract(%s,'^([0-9])+\\.([0-9])+\\.([0-9]+)',%s)+0,0)=%s"%(vfield,i+1,inv[i])
                    temp.append(vstr)
                selectedv="coalesce(regexp_extract(%s,'^([0-9])+\\.([0-9])+\\.([0-9]+)',%s)+0,0)%s%s"%(vfield,j,selectedValue,inv[j-1])
                if temp:
                    verlist.append("(%s and %s)" %(" and ".join(temp), selectedv) )
                else:
                    verlist.append(selectedv)

            verstr = " or ".join(verlist)

        elif selectedValue in ('='):
            verstr = " %s='%s' "%(vfield, inputValue.strip())

        return verstr

    def assembly_where(self, query, isreverse=False):
        # 这个地方真蛋疼
        #isreverse条件控制fpush_platform取反向值的情况
        todaystr = time.strftime('%Y-%m-%d',time.localtime())
        if isreverse:
            bpidstr = " key.fbpid='%s' and key.fpush_platform<>'%s' " %(query.get('bpid'), query.get("fpush_platform"))

        else:
            bpidstr = " key.fbpid='%s' and key.fpush_platform='%s' " %(query.get('bpid'), query.get("fpush_platform"))
            # bpidstr = " key.fbpid='%s' and key.fpush_platform='%s' " %(query.get('bpid'), query.get("fpush_platform"))
        condition =[]

        if str(query.get("user_type"))=='1':
            conditions = bpidstr

        elif str(query.get("user_type"))=='2':
            # uid是整型 uidstr = ",".join(query['uids'])
            # uid是字符串
            uidstr = ",".join(["'%s'" %uid.strip() for uid in query['uids'].split(',')])

            conditions = "%s  and key.fuid in (%s)  "%(bpidstr,uidstr)

        elif str(query.get("user_type"))=='3':
            data = query.get("tempsql", {})
            condstr = ""
            pushtime = query.get("time").get("begin_time")
            pushtime = int(pushtime)
            pushtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(pushtime))

            if data.get('1'):
                # 最后登录距今
                login_info= data.get('1')
                datestr = "datediff('%s',flogin_at)"%todaystr
                formatstr = " %s>=%s and %s<%s "%(datestr, login_info.get('begin'), datestr, login_info.get('end'))
                condition.append(formatstr)

            if data.get('2'):
                # 最后登录距推送发出
                push_info= data.get('2')
                datestr = "datediff('%s',flogin_at)"%pushtime
                formatstr = " %s>=%s and %s<%s "%(datestr, push_info.get('begin'), datestr, push_info.get('end'))
                condition.append(formatstr)

            if data.get('3'):
                # 注册距今
                sinup_info= data.get('3')
                datestr = "datediff('%s',fsignup_at)"%todaystr
                formatstr = " %s>=%s and %s<%s "%(datestr, sinup_info.get('begin'), datestr, sinup_info.get('end'))
                condition.append(formatstr)

            if data.get('4'):
                # 注册距推送发出
                spush_info= data.get('4')
                datestr = "datediff('%s',fsignup_at)"%pushtime
                formatstr = " %s>=%s and %s<%s "%(datestr, spush_info.get('begin'), datestr, spush_info.get('end'))
                condition.append(formatstr)

            if data.get('5'):
                lang_info= data.get('5')
                condition.append(" fplatformname = '%s' "%lang_info.get("selectedValue"))

            if data.get('6'):
                # 客户端版本|版本号
                ver_info= data.get('6')
                # condition.append(" %s%s'%s' "%('fversion_info', ver_info.get("selectedValue"), ver_info.get("inputValue")))
                condition.append(self.version('fversion_info',ver_info.get("selectedValue"),ver_info.get("inputValue") ) )

            if data.get('7'):
                # 游戏币
                coins = data.get('7')
                condition.append(" fuser_gamecoins >= %s and fuser_gamecoins < %s "%(coins.get("begin"), coins.get("end")))

            if data.get('8'):
                # 设备是否越狱
                coins = data.get('8')
                condition.append(" fm_dtype = '%s' "%(coins.get("selectedValue")))

            if data.get('9'):
                # 用户登入入口
                coins = data.get('9')
                condition.append(" fentrance_id = %s "%(coins.get("selectedValue")))

            if data.get('10'):
                # 历史付费
                paid_info = data.get('10')
                condition.append(" %s=%s "%(' fis_paid',paid_info.get("selectedValue")))

            if data.get('11'):
                # 是否是VIP级别
                paid_info = data.get('11')
                condition.append(" %s=%s "%(' fvip_level',paid_info.get("selectedValue")))

            if data.get('12'):
                # 性别
                paid_info = data.get('12')
                condition.append(" %s=%s "%(' fgender',paid_info.get("selectedValue")))

            if data.get('13'):
                sinup_info= data.get('13')
                # 注册后多少天未登陆
                datestr = "datediff('%s',fsignup_at)"%todaystr
                formatstr = " %s>=%s and %s<%s "%(datestr, sinup_info.get('begin'), datestr, sinup_info.get('end'))
                condition.append(formatstr)

            if data.get('14'):
                # 渠道ID|黑名单/白名单|(多个用逗号隔开)
                codes = ','.join([ " '%s' "%i for i in data.get('14')])
                condition.append(' %s(%s) '%('fchannel_code in', codes))

            if data.get('15'):
                sinup_info= data.get('15')
                # 玩牌总局数|至
                formatstr = " %s>=%s and %s<%s "%("fparty_num_total", sinup_info.get('begin'), "fparty_num_total", sinup_info.get('end'))
                condition.append(formatstr)

            if data.get('16'):
                sinup_info= data.get('16')
                # 当日玩牌局数|至
                formatstr = " %s>=%s and %s<%s "%("fparty_num", sinup_info.get('begin'), "fparty_num", sinup_info.get('end'))
                condition.append(formatstr)

            if data.get('17'):
                sinup_info= data.get('17')
                # 历史付费额度|至
                formatstr = " %s>=%s and %s<%s "%("fpay_num_total", sinup_info.get('begin'), "fpay_num_total", sinup_info.get('end'))
                condition.append(formatstr)

            if str(query.get('conditionType'))=='0':
                condstr = ' and '.join(["( %s )" %i for i in condition])

            elif str(query.get('conditionType')) == '1':
                condstr = ' or '.join(["( %s )" %i for i in condition])

            if condstr:
                conditions = "%s and %s"%(bpidstr,condstr)
            else:
                conditions = bpidstr

        elif str(query.get("user_type"))=='4':   #通过表关联fuid查数据,用户可以在diy系统上传表，然后用来关联
            # uid是整型 uidstr = ",".join(query['uids'])
            # uid是字符串
            tb_name = query['tb_name']   #tempwork.agg_mid
            field_name = query['field_name']
            conditions = """ inner join {tb_name} as tb
                        on key.fuid=tb.{field_name}
                        where
                        {bpidstr}  """.format(tb_name=tb_name, field_name=field_name, bpidstr=bpidstr)

            return conditions

        conditions = " %s%s "%(" where ",conditions)

        return conditions

    def assembly_sql(self, query):
        if '_except_' in query.get("fpush_platform"):
            platname = query.get("fpush_platform").split('_except_')
            query['fpush_platform'] = platname[0]
            query['conditions1'] = self.assembly_where(query)
            query['fpush_platform'] = platname[1]
            query['conditions2'] = self.assembly_where(query)

            tempsql="""insert overwrite table pushdb.push_user
                              partition(fpushid = '%(jobname)s')
                      select a.fuid, a.ftoken, a.fpush_platform from
                             (
                              select key.fuid, ftoken, key.fpush_platform
                                from stage.user_push_hbase_3rd %(conditions1)s
                               group by key.fuid, ftoken, key.fpush_platform
                             )  a

                                left join

                             (
                              select key.fuid, ftoken, key.fpush_platform
                                from stage.user_push_hbase_3rd %(conditions2)s
                               group by key.fuid, ftoken, key.fpush_platform
                              ) b
                           on a.ftoken=b.ftoken
                        where coalesce(b.ftoken,'')='' ;
                       select ftoken from
                       pushdb.push_user
                       where fpushid = '%(jobname)s'
                       group by ftoken""" %query

        elif query.get("fpush_platform") == 'boyaa_umeng':
            platname = query.get("fpush_platform").split('_')
            query['fpush_platform'] = platname[0]
            query['conditions1'] = self.assembly_where(query)
            query['fpush_platform'] = platname[1]
            query['conditions2'] = self.assembly_where(query)

            tempsql="""insert overwrite table pushdb.push_user
                              partition(fpushid = '%(jobname)s')
                      select a.fuid, a.ftoken, b.ftoken fpush_platform from
                             (
                              select key.fuid, ftoken, key.fpush_platform
                                from stage.user_push_hbase_3rd %(conditions1)s
                               group by key.fuid, ftoken, key.fpush_platform
                             )  a

                                left join

                             (
                              select key.fuid, ftoken, key.fpush_platform
                                from stage.user_push_hbase_3rd %(conditions2)s
                               group by key.fuid, ftoken, key.fpush_platform
                              ) b
                           on a.fuid=b.fuid;
                       select ftoken,max(fpush_platform) from
                       pushdb.push_user
                       where fpushid = '%(jobname)s'
                       group by ftoken""" %query

        else:
            conditions = self.assembly_where(query)
            query['conditions'] = conditions
            tempsql="""insert overwrite table pushdb.push_user
                       partition(fpushid = '%(jobname)s')
                       select key.fuid, ftoken, key.fpush_platform
                         from stage.user_push_hbase_3rd
                              %(conditions)s
                       group by key.fuid, ftoken, key.fpush_platform;
                       select ftoken from
                       pushdb.push_user
                       where fpushid = '%(jobname)s'
                       group by ftoken""" %query

        return tempsql


    def stat(self):
        """ 重要部分，统计内容 """
        mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
        data = mdb.getOne(""" select * from tjob_running where jid = %s """ %self.eid)

        # res = self.sql_exe(hql)
        # if res != 0:return res
        if isinstance(data, dict):
            # 把任务设定在tempjobs资源池
            # 控制map，reduce 的最大个数
            hivecmds = ["set mapreduce.job.queuename=tempjobs"]
            data['mapreduce_cnt'] = ';'.join(hivecmds)


        if data.get('jobtype','') in ('pushjobs'):
            s = ShellExec()

            query = ast.literal_eval(data.get('tempsql',''))  #data.get('tempsql','') 是一个字典

            sql = self.assembly_sql(query).split(";")
            # 开发数据和测试数据放到不同的目录
            data['dir'] = 'push_data_dev' if data['jobname'].find('dev')>=0 else 'push_data'
            data['tempsql'] = sql[0]
            sql_temp = "%(mapreduce_cnt)s; %(tempsql)s" % data
            # execmd1 = """source ~/.bash_profile; hive -e "%(mapreduce_cnt)s; %(tempsql)s" """%data
            # execmd1 = execmd1.encode('utf-8')

            data['tempsql'] = sql[1]
            data['mapreduce_cnt'] = "%s;%s"%(data['mapreduce_cnt'],"set hive.cli.print.header=false")

            execmd2 = """source ~/.bash_profile; hive -e "%(mapreduce_cnt)s; %(tempsql)s" > /data/workspace/push_mid/%(jobname)s.csv;"""%data
            execmd2 = execmd2.encode('utf-8')
            cmds = [
                    # {'cmd':execmd1,'timeout':data['maxtime']/2-60 },
                    {'cmd':execmd2,'timeout':data['maxtime']-120 },
                    {'cmd':"cd /data/workspace/push_mid/; sed -i 's/\\t/,/g' %(jobname)s.csv; gzip -f %(jobname)s.csv; cp %(jobname)s.csv.gz /data/workspace/%(dir)s/ "%data,'timeout':120}
                   ]

            res = self.hq.exe_sql(sql_temp)   #执行统计sql，将数据插入到pushdb.push_user，这里不适用命令行，因为命令行长度有限制，容易出错

            for item in cmds:
                print "push-cmd:{}, push-timeour:{}".format(item['cmd'], item['timeout'])
                out, err = s.execute(item['cmd'], item['timeout'])   #执行命令行，导出数据
                print "stdout：{}".format(out)
                print "stderr：{}".format(err)

        else:
            raise Exception(u"该任务不存在")




#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_pushjobs(statDate)
a()
