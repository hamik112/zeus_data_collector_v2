#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@domob.cn
Created Time: 2017-06-01 15:39:45
"""
import os
import sys
import time
import datetime
import pandas as pd

from multiprocessing import Process, cpu_count

exe_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
collector_path = os.path.realpath(exe_path + '/../')
sys.path.append(collector_path)

from collector.base_collector import BaseCollector
from adaccount_breakdown_working import AdAccountBreakdownWorking


class AdAccountBreakdownCollector(BaseCollector):
	def __init__(self, conf):
		super(AdAccountBreakdownCollector, self).__init__(conf)
		self.company_id = ''


	"""
	获取breakdown数据的主控制方法
	"""
	def run(self):
		try:
			data_path = self.data_path
			ad_account_path = "%s/%s/%s" % (data_path, self.dt, 'adaccount')
			if not os.path.exists(ad_account_path): os.makedirs(ad_account_path)

			access_token_list = [self.bf_access_token, self.dm_access_token]
			for access_token in access_token_list:
				business_ids = self.get_bmid_by_access_token(access_token)
				self.company_id	= self.company_info[access_token]
				if business_ids:
					for business_id in business_ids:
						self.get_accounts_by_bmid(business_id, access_token)
				

					account_working_list = []
					for process_num in range(cpu_count()):
						account_working	= AdAccountBreakdownWorking(self,  self.account_queue, ad_account_path)
						account_working_list.append(account_working)

					for account_working in account_working_list:
						account_working.daemon = True
						account_working.start()
					self.account_queue.join()
			print 'all done!'
		except Exception, why:
			print why




# vim: set noexpandtab ts=4 sts=4 sw=4 :
