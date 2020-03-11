#! /usr/local/python272/bin/python
# coding: utf-8
import os
import sys
import time
from sys import path
path.append('%s/service' % os.getenv('SCS_PATH'))

from hp_sync import Sync, get_date

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
                "    pg_to_hive.py table \n"
                "    pg_to_hive.py table date eid")
        sys.exit(1)

    s = Sync('import')
    cmd, err = s.execute(table)
    if err:
        raise Exception(err)
