import os

from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/jobs/hadoop' % os.getenv('SCS_PATH'))

#path.append('/tmp/scs-test/')

import mr_his_restful

print "**********Get Entity Diagnositc Information*****************"

print mr_his_restful.get_diag_logs_by_eid(129347,1439949917)

print "**********Get Entity Map Reduce Count*****************"

print mr_his_restful.get_mr_count_by_eid(129347,1439949917)

print "*******************************************************"


