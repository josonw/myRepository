#-*- coding: UTF-8 -*-
"""
数据中心，hive调用模板
所有的sql里的表名前面必须带上库名
@Liga

"""
# PG 参数
import os
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/tez/' % os.getenv('SCS_PATH'))

from libs import DB_PostgreSQL as pgdb
from libs import DB_Mysql as mysqldb

# Hive参数
from HiveMRSql import HiveMRSql
from BaseSparkSql import BaseSparkSql
from HiveOnTezSql import HiveOnTezSql
from PublicFunc import PublicFunc
import config

import sys
import time
import datetime

# 统一全局使用，不要每个脚本去连接了
pg_db = pgdb.Connection(host      = config.PG_DB_HOST,
                        database  = config.PG_DB_NAME,
                        user      = config.PG_DB_USER,
                        password  = config.PG_DB_PSWD)

mysql_db = mysqldb.Connection(host      = config.DB_HOST,
                              database  = config.DB_NAME,
                              user      = config.DB_USER,
                              password  = config.DB_PSWD)




class BaseSqlArgs():
    """docstring for ClassName"""
    def __init__(self, arg):
        self.args = {}
        # 统一获取时间变量函数
        self.stat_date = arg

    def create_date_args(self):
        stat_date = self.stat_date
        args_dict = {
            'statdatenum'             : stat_date.replace("-", ""),
            'statdate'                : stat_date,
            'ld_begin'                : stat_date,
            'ld_end'                  : PublicFunc.add_days(stat_date, 1),
            'ld_month_begin'          : PublicFunc.trunc(stat_date, "MM"),
            'ld_1month_ago_begin'     : PublicFunc.add_months(PublicFunc.trunc(stat_date, "MM"), -1),
            'ld_2month_ago_begin'     : PublicFunc.add_months(PublicFunc.trunc(stat_date, "MM"), -2),
            'ld_3month_ago_begin'     : PublicFunc.add_months(PublicFunc.trunc(stat_date, "MM"), -3),
            'ld_1month_after_begin'   : PublicFunc.add_months(PublicFunc.trunc(stat_date, "MM"), 1),
            'ld_month_end'            : PublicFunc.add_days(PublicFunc.add_months(PublicFunc.trunc(stat_date, "MM"), 1),-1),
            'ld_1month_ago_end'       : PublicFunc.add_days(PublicFunc.trunc(stat_date, "MM"),-1),
            'ld_2month_ago_end'       : PublicFunc.add_days(PublicFunc.add_months(PublicFunc.trunc(stat_date, "MM"), -1),-1),
            'ld_3month_ago_end'       : PublicFunc.add_days(PublicFunc.add_months(PublicFunc.trunc(stat_date, "MM"), -2),-1),
            'ld_week_begin'           : PublicFunc.trunc(stat_date, "IW"),
            'ld_1day_ago'             : PublicFunc.add_days(stat_date, -1),
            'ld_2day_ago'             : PublicFunc.add_days(stat_date, -2),
            'ld_3day_ago'             : PublicFunc.add_days(stat_date, -3),
            'ld_4day_ago'             : PublicFunc.add_days(stat_date, -4),
            'ld_5day_ago'             : PublicFunc.add_days(stat_date, -5),
            'ld_6day_ago'             : PublicFunc.add_days(stat_date, -6),
            'ld_7day_ago'             : PublicFunc.add_days(stat_date, -7),
            'ld_14day_ago'            : PublicFunc.add_days(stat_date, -14),
            'ld_29day_ago'            : PublicFunc.add_days(stat_date, -29),
            'ld_30day_ago'            : PublicFunc.add_days(stat_date, -30),
            'ld_60day_ago'            : PublicFunc.add_days(stat_date, -60),
            'ld_90day_ago'            : PublicFunc.add_days(stat_date, -90),
            'ld_season_begin'         : PublicFunc.trunc(stat_date, "q"),
            'ld_year_begin'           : PublicFunc.trunc(stat_date, "yyyy"),
        }
        self.args.update(args_dict)


    def create_default_args(self):
        import service.sql_const as sql_const
        args_dict = {
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_str_report':sql_const.NULL_STR_REPORT,
            'null_int_report':sql_const.NULL_INT_REPORT
        }
        self.args.update(args_dict)

    def create_stat_array(self):
        args_dict = {
        }
        self.args.update(args_dict)


    def __call__(self):
        """统计执行流程
        初始化，检测，建表，统计
        """
        self.create_date_args()
        self.create_default_args()
        return self.args



class BaseStatModel():
    """所有存储过程执行的父类

    编写的存储过程类继承该类。改写 create_tab, stat 方法
    """
    def __init__(self, args_list=[], **args_dict):
        """初始参数
        stat_date   传入的运算日期
        eid 记录错误日志
        """
        self.debug = False
        self.eid = 0
        self.stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")

        if len(args_list) >= 1:
            self.stat_date = args_list[0]

        if len(args_list) >= 2:
            self.eid = args_list[1]

        if len(args_list) >= 3:
            self.debug = True

        self.hql_list = []
        self.sql_args = {}


    def append(self, hql):
        self.hql_list.append(hql)


    def getCalling(self):
        calling =  '%s' % self.__class__
        return calling[9:]


    def hive_init(self):
        """实例化的HiveSql类
        """
        calling = self.getCalling()
        computer_engine = "MR"
        try:
            engine_rst = mysql_db.getOne("""select engine from task_computer_engine tce join job_entity je
                on tce.jid = je.jid where je.eid=%s"""%(self.eid));
            if engine_rst:
                if engine_rst['engine']:
                    computer_engine = engine_rst['engine']
        except Exception, e:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                "Failed to get computer engine of ",self.eid," task.",e
        finally:
            mysql_db.close()

        if computer_engine == "TEZ":
            self.hq = HiveOnTezSql(self.stat_date, calling,task_name=self.__class__.__name__,eid=self.eid)
        elif computer_engine == "SPARK":
             self.hq = BaseSparkSql(self.stat_date, calling,task_name=self.__class__.__name__,eid=self.eid)
        else:
            self.hq = HiveMRSql(self.stat_date, calling,task_name=self.__class__.__name__,eid=self.eid)


    def check_input(self):
        """参数检查部分
        @后续扩展。
        """
        try:
            time.strptime(self.stat_date, '%Y-%m-%d')
        except Exception, e:
            self.write_log("the format of stat date(" + self.stat_date + ") is error, must yyyy-mm-dd")
            return 2
        args = BaseSqlArgs(self.stat_date)
        self.sql_args = args()
        return 0


    def write_log(self, info = ''):
        """打印信息
        @后续扩展
        """
        print '[' + time.strftime('%F %X',time.localtime()) + '] ' + info


    def create_tab(self):
        """创建数据表，语句
        """
        pass


    def stat(self):
        """重要部分，统计内容，编写统计内容
        #调试的时候把调试开关打开，正式执行的时候设置为 True
        # hql结尾不要带分号';'
        # 如果要更新的分区已存在数据，该语法会覆盖select查询到的分区的结果，select没有查到的分区的信息不会更新
        #一级分区动态，二级分区动态
        #用更新分区式语法插入统计数据，如果用动态分区的方式插入数据，请执行如下语句
        """
        pass

    def sql_exe(self, sql):
        if type(sql) == list:
            for i in sql:
                result = self.hq.exe_sql(i % self.sql_args, self.debug)
                if result != 0:
                    break
        else:
            result = self.hq.exe_sql(sql % self.sql_args, self.debug)
        return result

    def sql_build(self, sql, args={} ):
        sql_args = {}
        sql_args.update(self.sql_args)
        if type(args) == dict:
            sql_args.update(args)
        return sql % sql_args

    def grouping_sets_list(self, is_hall_mode=0):
        """运算组合配置，不可随意调整。！！！！
        """
        GROUPSET = {
          'fields':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code'],
          'groups':[
                    [1,  1,  1,  1,  1,  1,  0],
                    [1,  1,  1,  1,  0,  0,  0],
                    [1,  1,  1,  0,  1,  1,  0],
                    [1,  1,  1,  0,  0,  0,  0],
                    [1,  1,  0,  0,  0,  0,  0],
                    [1,  0,  0,  1,  0,  0,  0]
            ]
        }
        # 非大厅
        GROUPSET_NOT_HALL = {
            'fields':['fgamefsk', 'fplatformfsk', 'fhallfsk', 'fgame_id', 'fterminaltypefsk', 'fversionfsk', 'fchannel_code'],
            'groups':[
                    [1,  1,  0,  0,  1,  1,  0],
                    [1,  1,  0,  0,  0,  0,  0]
                    ]
        }

        if is_hall_mode:
            return GROUPSET
        else:
            return GROUPSET_NOT_HALL


    def build_grouping_sets_list(self, is_hall_mode=1, extend_group = []):
        """生成组合list
        base_group = {fields:[a, b, c],
                      groups:[
                             [1, 1, 1],
                             [1, 1, 0],
                             [1, 0, 0]
                            ]
                    }
        extend_group 在base_group 的基础上，扩展组合。结构同上。
        """
        def clist(fields, filed_on_off):
            result = []
            for filed, on_off in zip(fields, filed_on_off):
                if on_off:
                    result.append(filed)
            return result
        base_group = self.grouping_sets_list(is_hall_mode)
        group_list = [ clist(base_group['fields'], i) for i in base_group['groups'] ]
        if extend_group:
            result = []
            extend_group_list = [ clist(extend_group['fields'], i) for i in extend_group['groups'] ]
            for ei in extend_group_list:
                result.extend([ gi + ei for gi in group_list])
        else:
            result = group_list
        return result


    def sql_build_grouping_sets(self, sql, group_list, group_num=3):
        """用于新后台，多组合数据统计
        group_list 组合
        group_num 分组数量, 默认3分组
        group_list = [ [ [g,p,v],[g,p],[g] ],
                         [ [],[],[] ],
                         [ [],[],[] ]
                        ]
        """
        import math
        def chunks(arr, n):
            return [arr[i:i+n] for i in range(0, len(arr), n)]

        sql_list = []
        group_set_list = chunks(group_list, group_num)
        for gsl in group_set_list:
            group_set = ['(%s)' % ','.join(map(str, i)) for i in gsl]
            sql_list.append(sql + """
                grouping sets (
                %s
                ) """ % ',\n'.join(group_set))
        return sql_list



    def sql_template_build(self, **args):
        """sql 构造，构造出最终执行sql，
        处理，大厅，非大厅两种模式的grouping sets 的组合

        提高grouping sets 语法的效率，增加 jobs 数量，技术细节参看以下链接。
        http://dccrazyboy.me/2015/05/16/hivezhong-grouping-setsyong-fa-ji-xing-neng-you-hua/

        sql 基础sql模板
        extend_group 除基础组合外的扩展组合
        group_num 单个jobs，组合数量，默认值3.
        """
        sql = args.get('sql', "").strip(';')
        extend_group = args.get('extend_group', {})
        group_num = args.get('group_num', 3)
        sql_list = []
        for is_hall in [0, 1]:
            self.sql_args['hallmode'] = is_hall
            group_list = self.build_grouping_sets_list(is_hall, extend_group)
            sqls = self.sql_build_grouping_sets(sql % self.sql_args, group_list, group_num)
            sql_list.extend(sqls)
        result = """
         union all
        """.join(sql_list)
        self.sql_args['sql_template'] = result



    def __call__(self):
        """统计执行流程
        初始化，检测，建表，统计
        """
        self.hive_init()
        self.check_input()
        self.create_tab()
        self.stat()



class BasePGStat():
    """所有PG计算的基础类"""

    def __init__(self, stat_date=datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d"), eid = 0):
        self.pg = pg_db
        self.stat_date = stat_date
        self.eid = eid
        self.get_paras()        # 参数获取

        self.hql_list = []
        self.hql_dict = self.create_date_args()
        self.sql_dict  = self.hql_dict
        self.getCalling()

    def get_paras(self):
        if len(sys.argv) == 1:
            #没有输入参数的话，日期默认取昨天
            self.stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        elif len(sys.argv) >= 3:
            #从输入参数取，实际是第几个就取第几个
            args = sys.argv[1].split(',')
            self.stat_date = args[0]
            self.eid = int(sys.argv[2])
        else :
            args = sys.argv[1].split(',')
            self.stat_date = args[0]

    def append(self, hql):
        self.hql_list.append(hql)


    def exe_hql(self, hql):
        # try:
        print hql
        self.pg.execute(hql)
        # except Exception, e:
        #     self._error_log(hql, str(e))
        #     print e
        #     sys.exit(1)


    def _error_log(self, sql, info):
        """ 错误信息打印函数，仅内部使用 """
        ## 调度系统日志记录到mysql
        try:
            log = """%s<br>%s""" % (sql,info)
            log = log.replace("'",'"')
            log = log.replace("\n",'<br>')
            in_log = """INSERT INTO `job_log` (`eid`,`err_log`,`ftime`) VALUES (%s, '%s', unix_timestamp(now()))""" % (self.eid,log)
            mysql_db.execute(in_log)
            mysql_db.close()
        except Exception, e:
            pass

        _path = sys.path[0]
        base_path = os.getenv('SCS_PATH')
        file_name = '%s/logs/error_%s.log' % (base_path, self.stat_date)

        try:
            f = open( file_name, 'a')
            print >> f, '%s : %s' % (self.calling, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) )
            print >> f, sql
            print >> f, info
            print >> f, ' '
            f.close()
        except Exception, e:
            pass


    def getCalling(self):
        calling =  '%s' % self.__class__        # __main__.agg_pg_paycenter_config
        self.calling = '%s.py' % calling[9:]

    """
    #执行多条语句
    sql = "insert into user (uid, name) values (%(uid)s, %(name)s)"
    data_2 = [{'uid':5, 'name':'boyaa_1'}, {'uid':6, 'name':'boyaa_2}]
    executemany(sql, data_2)
    """
    def exemany_hql(self, hql, parameters):
        try:
            self.pg.executemany(hql, parameters)
        except Exception, e:
            print e
            sys.exit(1)


    def query(self, hql):
        datas = self.pg.query(hql)
        return datas

    def exe_hql_list(self, hql_list=[]):
        temp_hql_list = []

        if hql_list:
            temp_hql_list = hql_list
        else:
            temp_hql_list = self.hql_list

        for hql_tmp in temp_hql_list:
            #函数有退出机制,不需要做异常判断
            self.exe_hql(hql_tmp)



    def stat(self):
        raise NotImplementedError("Subclasses should implement this!")

    def __call__(self):
        """统计执行流程
        """
        self.stat()
