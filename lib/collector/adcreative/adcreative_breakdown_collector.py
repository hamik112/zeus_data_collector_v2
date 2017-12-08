#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@xxx.cn
Created Time: 2017-06-21 15:03:56
"""

import os
import sys
import multiprocessing

exe_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
collector_path = os.path.realpath(exe_path + '/../')
sys.path.append(collector_path)

from collector.base_collector import BaseCollector
from adcreative_breakdown_working import AdCreativeBreakdownWorking


class AdCreativeBreakdownCollector(BaseCollector):
    def __init__(self, conf, args):
        super(AdCreativeBreakdownCollector, self).__init__(conf, args)
        self.company_id		= ''


    """
    获取ads breakdown数据的主控制方法
    """
    def run(self):
        try:
            data_path = self.data_path
            ad_creative_path = "%s/%s/%s" % (data_path, self.topic, self.dt)
            print 'ad_creative_path:%s' % ad_creative_path
            if not os.path.exists(ad_creative_path): os.makedirs(ad_creative_path)

            print 'Getting ads breakdown data...'

            access_token_list = [self.bf_access_token, self.dm_access_token]

            for access_token in access_token_list:
                business_ids = self.get_bmid_by_access_token(access_token)
                self.company_id	= self.company_info[access_token]

                if business_ids:
                    for business_id in business_ids:
                        self.get_accounts_by_bmid(business_id, access_token)

                    account_working_list = []

                    for process_num in range(multiprocessing.cpu_count()):
                        account_working	= AdCreativeBreakdownWorking(self, self.account_queue, ad_creative_path, access_token)
                        account_working_list.append(account_working)
                    for account_working in account_working_list:
                        account_working.daemon = True
                        account_working.start()
                    self.account_queue.join()
        except Exception, why:
            print why

# vim: set noexpandtab ts=4 sts=4 sw=4 :
