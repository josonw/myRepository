#-*- coding: UTF-8 -*-
#
import os
import sys

shpath = os.getenv('SCS_PATH')
cmd = "sh %s/jobs/test/flush_memcached.sh" %shpath
print cmd
print os.popen( cmd ).read()
