#! /usr/local/python272/bin/python
# coding: utf-8
import os
import sys
import time
from sys import path
path.append('%s/service' % os.getenv('SCS_PATH'))

from hp_sync_pgcluster import Sync, get_date


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
                "    hive_to_pg.py table \n"
                "    hive_to_pg.py table date \n"
                "    hive_to_pg.py table date eid")
        sys.exit(1)

    s = Sync('export')
    cmd, e = s.execute(table, date)
    if e:
        raise Exception(e)

