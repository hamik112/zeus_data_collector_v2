#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@xxx.cn
Created Time: 2017-06-06 14:20:04
"""
import os
import sys
import time
import json
import datetime
import pandas as pd
import threading
import logging
import datetime


class AdAccountBreakdownWorking(threading.Thread):
	def __init__(self, collector, account_queue, ad_account_path, **kwargs):
		threading.Thread.__init__(self, **kwargs)
		self.account_queue	= account_queue
		self.api_version	= collector.api_version
		self.fb_api     	= collector.fb_api
		self.start_dt		= collector.start_dt
		self.stop_dt    	= collector.stop_dt
		self.year       	= collector.year
		self.month      	= collector.month
		self.week       	= collector.week
		self.dt         	= collector.dt
		self.company_id		= collector.company_id
		self.logger			= collector.logger
		self.wflogger		= collector.wflogger
		self.ad_account_path	= ad_account_path
		self.class_name		= self.__class__.__name__


	
	"""
	获取Account Breakdown数据
	"""
	def get_adaccount_breakdown_insights(self, account_id, breakdown_attribute):
		try:
			breakdown_insight = self.fb_api.get_account_breakdown_insight(account_id, breakdown_attribute, self.start_dt, self.stop_dt) 
			if not breakdown_insight: return False
			self.logger.info('[%s] [%s] account_id:%s, breakdown_attribute:%s, breakdown_insight:%s' % \
				(self.class_name, 'get_adaccount_breakdown_insights', account_id, breakdown_attribute, breakdown_insight))
			return breakdown_insight
		except Exception, why:
			self.wflogger.exception('[%s] [%s] Exception, account_id:%s, breakdown_attribute:%s, breakdown_insight:%s, reason:%s' % \
				(self.class_name, 'get_adaccount_breakdown_insights', account_id, breakdown_attribute, breakdown_insight, why))
			return False


	"""
	获取BM id
	"""
	def get_account_business_id(self, account_id):
		try:
			business_info = self.fb_api.get_account_business_info(account_id)
			self.logger.info('[%s] [%s] account_id:%s, business_info:%s' % \
				(self.class_name, 'get_account_business_id', account_id, business_info))
			return business_info['id']
		except Exception, why:
			self.wflogger.info('[%s] [%s] Exception, account_id:%s, reason:%s' % \
				(self.class_name, 'get_account_business_id', account_id, why))
			return False


	"""
	获取account status
	"""
	def get_account_status(self, account_id):
		try:
			account_status = self.fb_api.get_account_status(account_id)
			if account_status:
				self.logger.info('[%s] [%s] account_id:%s, account_status:%s' % \
					(self.class_name, 'get_account_status', account_id, 'Active'))
				return True
			else:
				self.logger.info('[%s] [%s] account_id:%s, account_status:%s' % \
					(self.class_name, 'get_account_status', account_id, 'Not Active'))
				return False
		except Exception, why:
			self.wflogger.exception('[%s] [%s] Exception, account_id:%s, reason:%s') % \
				(self.class_name, 'get_account_status', account_id, why)
			return False

	
	"""
	获取统计breakdown数据
	"""
	def get_breakdown_stats_list(self, account_info, breakdown):
		try:
			(account_id, business_id, access_token) = account_info
			account_breakdown_insights	= self.get_adaccount_breakdown_insights(account_id, breakdown)
			account_category_info		= self.fb_api.get_account_category(account_id)
			breakdown_list	= self._build_breakdown_list(business_id, breakdown, account_breakdown_insights, account_category_info)
			return breakdown_list
		except Exception, why:
			self.wflogger.exception('[%s] [%s] Exception, account_id:%s, breakdown:%s, reason:%s' % \
				(self.class_name, 'get_breakdown_stats_list', account_id, breakdown, why))
			return False

	

	"""
	返回一个默认的breakdown数据字典
	"""
	def _init_breakdown_value(self):
		account_breakdown_dict = {}
		account_breakdown_dict = {
			"action_type"	:"",
			"action_value"	:"",
			"clicks"		:0,
			"impressions"	:0,
			"mobile_app_install" :0,
			"fb_mobile_add_to_cart"	:0,
			"fb_mobile_purchase"	:0,
			"fb_onsite_conversion_purchase":0,
			"category"		:"",
			"sub_category"	:"",
			"age"			:0,
			"country"		:"",
			"gender"		:"",
			"hourly_stats_aggregated_by_advertiser_timezone":"",
			"hourly_stats_aggregated_by_audience_timezone"	:"",
			"impression_device"	:"",
			"place_page_id"		:"",
			"publisher_platform":"",
			"device_platform"	:"",
			"region"			:""
		}
		return account_breakdown_dict


	"""
	重组breakdown数据,返回列表
	"""
	def _build_breakdown_list(self, business_id, breakdown, account_breakdown_insights, account_category_info):
		try:
			account_breakdown_value = self._init_breakdown_value()
			account_breakdown_keys = ["clicks", "impressions", "reach", "spend", "cpc", "cpm", "cpp", "ctr"]
			account_breakdown_list = []
			if not account_breakdown_insights: 
				return account_breakdown_list

			for ads_insight in account_breakdown_insights:
				account_breakdown_info = []
				account_breakdown_info.append(ads_insight['account_id'])
				account_breakdown_info.append(ads_insight['account_name'])
				account_breakdown_info.append(business_id)
				#company_id主要是为了区分蓝瀚和多盟数据
				account_breakdown_info.append(self.company_id)
				for account_breakdown_key in account_breakdown_keys:
					account_breakdown_info.append(ads_insight[account_breakdown_key])

				account_action_keys = ['mobile_app_install', 'fb_mobile_add_to_cart', 'fb_mobile_purchase', 'onsite_conversion_purchase']
				account_action_list = []
				for i in account_action_keys: account_action_list.append('')
				if "actions" in ads_insight.keys():
					for action in ads_insight["actions"]:
						if action["action_type"] == "mobile_app_install":
							index = account_action_keys.index('mobile_app_install')
							account_action_list[index] = action["value"]

						if action["action_type"] == "app_custom_event.fb_mobile_add_to_cart":
							index = account_action_keys.index('fb_mobile_add_to_cart')
							account_action_list[index] = action["value"]
						if action["action_type"] == "app_custom_event.fb_mobile_purchase":
							index = account_action_keys.index('fb_mobile_purchase')
							account_action_list[index] = action["value"]
						if action["action_type"] == "onsite_conversion.purchase":
							index = account_action_keys.index('onsite_conversion_purchase')
							account_action_list[index] = action["value"]
				account_breakdown_info.extend(account_action_list)
				account_breakdown_info.append(account_category_info["category"])
				account_breakdown_info.append(account_category_info["subcategory"])
				account_breakdown_attr = ['age', 'country', 'gender', 'hourly_stats_aggregated_by_advertiser_timezone', 'hourly_stats_aggregated_by_audience_timezone', 'impression_device', 'place_page_id', 'publisher_platform', 'device_platform', 'region']
				breakdown_attr_list = []
				for i in range(len(account_breakdown_attr)): breakdown_attr_list.append('')
				for attr in account_breakdown_attr:
					attr_index = account_breakdown_attr.index(attr)
					if attr in ads_insight.keys(): 
						breakdown_attr_list[attr_index] = ads_insight[attr]
				account_breakdown_info.extend(breakdown_attr_list)
				account_breakdown_info.extend([self.year, self.month, self.week, self.dt])
				account_breakdown_list.append(account_breakdown_info)	
			return account_breakdown_list
		except Exception, why:
			self.wflogger.exception('[%s] [%s] Exception, breakdown:%s' % (self.class_name, '_build_breakdown_list', breakdown))
			return False



	"""
	写入CSV方法
	"""
	def save_breakdown_data(self, account_id, account_breakdown_stats):
		try:
			data_obj = pd.DataFrame(account_breakdown_stats)
			file_path = '%s/%s.csv' % (self.ad_account_path, account_id)
			data_obj.to_csv(file_path, index=False, sep=',', header=False)
			
			return True
		except Exception, why:
			self.wflogger.exception('[%s] [%s] Exception, account_id:%s, account_breakdown_stats:%s, date:%s, reason:%s' % \
				(self.class_name, 'save_breakdown_data', account_id, account_breakdown_stats, self.dt, why))
			return False



	def run(self):
		breakdown_data  = {}
		breakdown_list = ['age', 'country', 'gender', 'hourly_stats_aggregated_by_advertiser_time_zone', 'hourly_stats_aggregated_by_audience_time_zone', 'impression_device', 'place_page_id', 'publisher_platform', 'device_platform', 'region']
		
		try:
			for account_info in iter(self.account_queue.get, None):
				(account_id, business_id, access_token)	= account_info
				self.logger.info('[%s] [%s] queue size:%d' % (self.class_name, 'run', self.account_queue.qsize()))
				#判断account status，只获取account状态为Active状态的breakdown数据
				account_status = self.get_account_status(account_id)
				if not account_status:	
					continue
					self.account_queue.task_done

				account_breakdown_stats_list = []
				for breakdown in breakdown_list:
					breakdown_stats_list = self.get_breakdown_stats_list(account_info, breakdown)
					for breakdown_stats in breakdown_stats_list: 
						account_breakdown_stats_list.append(breakdown_stats)
				self.save_breakdown_data(account_id, account_breakdown_stats_list)
				self.account_queue.task_done()
		except Exception, why:
			self.wflogger.exception('[%s] [%s] Exception, reason:%s' % (self.class_name, 'run', why))



# vim: set noexpandtab ts=4 sts=4 sw=4 :
