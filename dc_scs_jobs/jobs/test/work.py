#-*- coding: UTF-8 -*-
import os
import time
import datetime
import random
from optparse import OptionParser

def main():
	opt_parser = OptionParser()
	opt_parser.add_option("-d", "--date", dest="date", help="指定运算哪天的数据")
	opt_parser.add_option("-c", "--call", dest="call", help="指定调用的程序")
	(options, args) = opt_parser.parse_args()
	if options.date and options.call:
		date = options.date
		call = options.call
		print 'working date = %s | call = %s' % (date, call)
		stime = random.randint(20,30)
		time.sleep(stime)
		
	else:
		print 'parser Error'
	
# Main
if __name__ == '__main__':
	print('Main:',os.getpid())
	main()

##END