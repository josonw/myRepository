#! /usr/local/python272/bin/python
# coding: utf-8
import os
import sys
import time
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from stat_sync_pgcluster import Sync, get_date

def is_valid_date(datestr):
    try:
        time.strptime(datestr, "%Y-%m-%d")
        return True
    except:
        return False

if __name__ == '__main__':
    if len(sys.argv) == 4:
        table = sys.argv[1]
        date = sys.argv[2]
        eid = sys.argv[3]

        if not is_valid_date(date):
            print "date is invalid"
            sys.exit(1)
    elif len(sys.argv) == 3:
        table = sys.argv[1]
        date = sys.argv[2]
        eid = 0
        if not is_valid_date(date):
            print "date is invalid"
            sys.exit(1)
    elif len(sys.argv) == 2:
        table = sys.argv[1]
        date = get_date(-1)
        eid = 0
    else:
        print  ("Usage: \n"
                "    sqoop_export.py table \n"
                "    sqoop_export.py table date \n"
                "    sqoop_export.py table date eid")
        sys.exit(1)

    s = Sync('export')
    cmd, err = s.execute(table, date)
    if err:
        raise Exception(err)

