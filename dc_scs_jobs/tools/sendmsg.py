#-*- coding: UTF-8 -*-
import os
import sys
#把 str 编码由 ascii 改为 utf8 (或 gb18030)
reload(sys)
sys.setdefaultencoding( "utf-8" )

base_path = os.getenv('DC_SERVER_PATH')
from sys import path
path.append( base_path )

from libs.warning_way import send_sms
from dc_scs_jobs import config


if __name__ == '__main__':
    warning_msg = '【调度系统】检测调度系统未运行，重启中……'
    send_sms(config.CONTACTS_LIST_ENAME, warning_msg)
    print 'send_sms success!'