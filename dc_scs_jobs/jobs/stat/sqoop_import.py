#! /usr/local/python272/bin/python
# coding: utf-8
import os
import sys
import time
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from stat_sync import Sync, get_date

if __name__ == '__main__':
    if len(sys.argv) == 4:
        table = sys.argv[1]
        date = None
        eid = sys.argv[3]
    elif len(sys.argv) == 2:
        table = sys.argv[1]
        eid = 0
    else:
        print  ("Usage: \n"
                "    sqoop_import.py table \n"
                "    sqoop_import.py table date eid")
        sys.exit(1)

    s = Sync('import')
    cmd, err = s.execute(table)
    if err:
        raise Exception(err)
