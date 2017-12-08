#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@xxx.cn
Created Time: 2017-06-23 18:14:00
"""
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

import threading
import Queue

from adcreative_base import AdCreativeBase
from collector.status_collector import AccountStatus


class AdCreativeAllWorking(AdCreativeBase):
    def __init__(self, collector, account_queue, adcreative_path, access_token, **kwargs):
        super(AdCreativeAllWorking, self).__init__(collector, account_queue, adcreative_path, access_token, **kwargs)
        self.class_name		= self.__class__.__name__


    """
    获取Ad 所有Insights 信息
    """
    def _get_ad_insights(self, ad_id, start_dt, stop_dt):
        try:
            ad_insights = self.fb_api.get_ad_insights(ad_id, '', start_dt, stop_dt)
            if ad_insights:
                self.logger.info('[%s] [%s] Info, ad_id:%s, start_dt:%s, stop_dt:%s, ad_insights:%s' % \
                    (self.class_name, '_get_ad_insights', ad_id, start_dt, stop_dt, ad_insights))
                return ad_insights
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, ad_id:%s, start_dt:%s, stop_dt:%s, reason:%s' % \
                (self.class_name, '_get_ad_insights', ad_id, start_dt, stop_dt, why))
            return False



    """
    按格式构建Insights数据
    """
    def _build_insights_data(self, ad_insights, account_category_info):
        try:
            insights_data_list = []
            if ad_insights:
                for ad_insight in ad_insights:
                    ad_insights_data = []
                    ad_insights_data.append(self.company_id)
                    ad_insights_data.append(self.business_id)
                    ad_parent_keys = ['account_id', 'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name']
                    for ad_parent_key in ad_parent_keys:
                        ad_insights_value = ad_insight[ad_parent_key]
                        ad_insights_value = ad_insights_value.replace(' ', '').replace('"', '').replace(",", '_')
                        ad_insights_data.append(ad_insights_value)

                    ad_insight_keys = ['buying_type', 'clicks', 'cpc', 'cpm', 'cpp', 'ctr', 'frequency', 'impressions']
                    for ad_insight_key in ad_insight_keys:
                        if ad_insight_key in ad_insight:
                            ad_insights_data.append(ad_insight[ad_insight_key])
                        else:
                            ad_insights_data.append(0)

                    mobile_app_install				= 0
                    fb_mobile_add_to_cart			= 0
                    fb_mobile_purchase				= 0
                    fb_onsite_conversion_purchase	= 0
                    if "actions" in ad_insight.keys():
                        actions = ad_insight["actions"]
                        for action in actions:
                            if action["action_type"] == "mobile_app_install":
                                mobile_app_install	= action["value"]
                            if action["action_type"] == "app_custom_event.fb_mobile_add_to_cart":
                                fb_mobile_add_to_cart	= action["value"]
                            if action["action_type"] == "app_custom_event.fb_mobile_purchase":
                                fb_mobile_purchase	= action["value"]
                            if action["action_type"] == "onsite_conversion.purchase":
                                fb_onsite_conversion_purchase = action["value"]

                    ad_insights_data.append(mobile_app_install)
                    ad_insights_data.append(ad_insight["spend"])
                    ad_insights_data.append(fb_mobile_add_to_cart)
                    ad_insights_data.append(fb_mobile_purchase)
                    ad_insights_data.append(fb_onsite_conversion_purchase)
                    ad_insights_data.extend([
                        account_category_info["category"],
                        account_category_info["subcategory"]
                    ])
                    insights_data_list.append(ad_insights_data)
            self.logger.info('[%s] [%s] Info, ad_insights:%s, insights_data_list:%s' % \
                (self.class_name, '_build_insights_data', ad_insights, insights_data_list))
            return insights_data_list
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, ad_insights:%s, reason:%s' % \
                (self.class_name, '_build_insights_data', ad_insights, why))
            return False



    """
    构建保存数据的方法，返回列表
    """
    def _build_collector_data(self, ad_insights, ad_creatives, account_category_info):
        try:
            collector_data_list = []
            if ad_insights and ad_creatives:
                creative_data	= self._build_creative_data(ad_creatives)
                insights_data	= self._build_insights_data(ad_insights, account_category_info)
                date_data		= [self.start_dt, self.stop_dt, self.dt]

                if insights_data and creative_data:
                    for insight_data in insights_data:
                        collector_data = []
                        collector_data.extend(insight_data)
                        collector_data.extend(creative_data)
                        collector_data.extend(date_data)
                        collector_data_list.append(collector_data)
            self.logger.info('[%s] [%s] Info, ad_insights:%s, ad_creatives:%s, account_category_info:%s, collector_data_list:%s' % \
                (self.class_name, '_build_collector_data', ad_insights, ad_creatives, account_category_info, collector_data_list))
            return collector_data_list
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, ad_insights:%s, ad_creatives:%s, account_category_info:%s, reason:%s' % \
                (self.class_name, '_build_collector_data', ad_insights, ad_creatives, account_category_info, why))
            return False


    """
    保存ads数据
    """
    def _save_collect_data(self, ads_info_list, account_category_info, file_path):
        try:
            #声明保存数据的header
            header = ['company_id', 'business_id', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name',
                'buying_type', 'clicks', 'cpc', 'cpm', 'cpp', 'ctr', 'frequency', 'impressions', 'mobile_app_install', 'spend',
                'fb_mobile_add_to_cart', 'fb_mobile_purchase', 'fb_onsite_conversion_purchase', 'category', 'sub_category', 'image_url', 'image_hash',
                'video_id', 'promoted_url', 'link', 'start_dt', 'stop_dt', 'dt'
            ]
            if ads_info_list:
                save_data_list = []
                for ad in ads_info_list:
                    ad_insights = self._get_ad_insights(ad["id"], self.start_dt, self.stop_dt)
                    #如果ad insight为空，就不要抓了
                    if not ad_insights: continue
                    ad_creative = self._get_ad_creatives(ad["id"])
                    collector_data_list = self._build_collector_data(ad_insights, ad_creative, account_category_info)
                    if collector_data_list:
                        for collector_data in collector_data_list:
                            save_data_list.append(collector_data)
                #如果抓取到的数据为空则不保存
                if save_data_list:
                    self.collector.save_list_data(save_data_list, header, file_path)
                    return True
            return False
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, ads_info_list:%s, reason:%s' % \
                (self.class_name, '_save_collect_data', ads_info_list, why))
            return False


    """
    线程抓取的主控制方法
    """
    def run(self):
        while True:
            try:
                self.logger.info('thread_count:%d, thread_name:%s' % (threading.active_count(), threading.current_thread().getName()))
                (account_id, business_id) = self.account_queue.get(False)
                self.logger.info('[%s] [%s] account queue size:%d' % (self.class_name, 'run', self.account_queue.qsize()))
                self.account_id		= account_id
                self.business_id	= business_id

                #判断Account Status，如果非Active状态，则直接跳过抓取
                account_detail = self._get_account_info(account_id)
                if not account_detail or account_detail['account_status'] != AccountStatus.ACTIVE.value:
                    self.account_queue.task_done()
                    continue

                #保存文件的路径
                file_path = '%s/%s.csv' % (self.adcreative_path, account_id)
                account_category_info = self._get_account_category(account_id)
                #获取Campaign Insight数据，如果为空则不获取其ad insights
                campaigns_insights = self._get_campaign_insights_by_account(account_id, '', self.start_dt, self.stop_dt)
                if campaigns_insights:
                    ads_info_list = []
                    for campaigns_insight in campaigns_insights:
                        campaign_id		= '' if "campaign_id" not in campaigns_insight else campaigns_insight["campaign_id"]
                        #获取Adsets Insights, 如果为空则不获取其ad insights
                        adsets_insights = self._get_adset_insights_by_campaign(campaign_id, '', self.start_dt, self.stop_dt)
                        if adsets_insights:
                            for adsets_insight in adsets_insights:
                                adset_id		= '' if "adset_id" not in adsets_insight else adsets_insight["adset_id"]
                                ads_info = self._get_ads_info_by_adset(adset_id)
                                if ads_info: ads_info_list.extend(ads_info)
                    self._save_collect_data(ads_info_list, account_category_info, file_path)
                self.account_queue.task_done()
            except Queue.Empty:
                self.logger.info('[%s] [%s] Info, account_queue size is Empty!' % (self.class_name, 'run'))
                return False
            except Exception, why:
                self.wflogger.exception('[%s] [%s] Exception, reason:%s' % (self.class_name, 'run', why))
                self.account_queue.task_done()
# vim: set noexpandtab ts=4 sts=4 sw=4 :
