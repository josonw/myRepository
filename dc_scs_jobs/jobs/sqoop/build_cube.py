#! /usr/local/python272/bin/python
# coding: utf-8

import sys
import os
import time
import json

sys.path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs import kylin_query

if __name__ == "__main__" :

    print sys.argv
    if len(sys.argv) < 3:
        print """ Usage:
                       python build_cube.py  '' 'cardtype_un_cube' '2017-01-16' '525596'
              """
        sys.exit(-1)

    cube_name = sys.argv[2]

    if not cube_name:
        print "args error"
        sys.exit(-2)

    print cube_name

    result = kylin_query.build_cube(cube_name)

    uuid = ''

    try:
        jr = json.loads(result)
        uuid = jr.get('uuid')
    except:
        print '-----------parse json faild------------'
        print result
        sys.exit(-3)



    for  i in range(15):
        status = kylin_query.get_job_status(uuid)

        print status

        try :
            jstatus = json.loads(status)

            if jstatus.get('job_status') == "FINISHED":
                break

            if jstatus.get('job_status') == "ERROR":
                print "job exit with error"
                sys.exit(-4)

        except:
            print "get status failed %d times" % i

        time.sleep(120)

    else:
        print "job  timeout"
        sys.exit(100)

    print '-----------DONE-------------'








