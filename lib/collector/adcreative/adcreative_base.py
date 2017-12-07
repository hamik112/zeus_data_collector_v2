#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@domob.cn
Created Time: 2017-06-23 18:02:37
"""
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

import time
import json
import datetime
import pandas as pd
import threading
import logging
import datetime
import requests
import json


from collector.status_collector import AccountStatus

class AdCreativeBase(threading.Thread):
    def __init__(self, collector, account_queue, adcreative_path, access_token, **kwargs):
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
        self.adcreative_path= adcreative_path
        self.collector		= collector
        self.account_id		= ''
        self.business_id	= ''
        self.access_token	= access_token
        self.class_name		= self.__class__.__name__


    """
    获取Account 详细信息
    """
    def _get_account_info(self, account_id):
        try:
            account_info = self.fb_api.get_account_info(account_id)
            if account_info:
                self.logger.info('[%s] [%s] Info, account_id:%s, account_info:%s' % \
                    (self.class_name, '_get_account_info', account_id, account_info))
                return account_info
            raise Exception('Facebook API get account info Error!')
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, account_id:%s, reason:%s' % \
                (self.class_name, '_get_account_info', account_id, why))
            return False


    """
    获取category 信息
    """
    def _get_account_category(self, account_id):
        try:
            category = self.fb_api.get_account_category(account_id)
            if category:
                self.logger.info('[%s] [%s] Info, account_id:%s, category:%s' % \
                    (self.class_name, '_get_account_category', account_id, category))
                return category
            raise Exception('Facebook API get account category Error!')
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, account_id:%s, reason:%s' % \
                (self.class_name, '_get_account_category', account_id, why))
            return False



    """
    通过account id获取campaigns 详细信息
    """
    def _get_campaign_info_by_account(self, account_id):
        try:
            campaigns_info = self.fb_api.get_campaigns_info_by_account(account_id)
            if campaigns_info:
                self.logger.info('[%s] [%s] Info, account_id:%s, ads_info:%s' % \
                    (self.class_name, '_get_campaign_info_by_account', account_id, campaigns_info))
                return campaigns_info
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, account_id:%s, reason:%s' % \
                (self.class_name, '_get_campaign_info_by_account', account_id, why))
            return False

    """
    通过campaign id获取adset 详细信息
    """
    def _get_adset_info_by_campaign(self, campaign_id):
        try:
            adsets_info = self.fb_api.get_adsets_info_by_campaign(campaign_id)
            if adsets_info:
                self.logger.info('[%s] [%s] Info, campaign_id:%s, ads_info:%s' % \
                    (self.class_name, '_get_adsets_info_by_campaign', campaign_id, adsets_info))
                return adsets_info
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, campaign_id:%s, reason:%s' % \
                (self.class_name, '_get_adsets_info_by_campaign', campaign_id, why))
            return False



    """
    通过account id获取ad 详细信息
    """
    def _get_ads_info_by_account(self, account_id):
        try:
            ads_info = self.fb_api.get_ads_info_by_account(account_id)
            if ads_info:
                self.logger.info('[%s] [%s] Info, account_id:%s, ads_info:%s' % \
                    (self.class_name, '_get_ads_info', account_id, ads_info))
                return ads_info
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, account_id:%s, reason:%s' % \
                (self.class_name, '_get_ads_info_by_account', account_id, why))
            return False


    """
    通过campaign id获取ad 详细信息
    """
    def _get_ads_info_by_campaign(self, campaign_id):
        try:
            ads_info = self.fb_api.get_ads_info_by_campaign(campaign_id)
            if ads_info:
                self.logger.info('[%s] [%s] Info, campaign_id:%s, ads_info:%s' % \
                    (self.class_name, '_get_ads_info_by_campaign', campaign_id, ads_info))
                return ads_info
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, campaign_id:%s, reason:%s' % \
                (self.class_name, '_get_ads_info_by_campaign', campaign_id, why))
            return False


    """
    通过adset id获取ad 详细信息
    """
    def _get_ads_info_by_adset(self, adset_id):
        try:
            ads_info = self.fb_api.get_ads_info_by_adset(adset_id)
            if ads_info:
                self.logger.info('[%s] [%s] Info, adset_id:%s, ads_info:%s' % \
                    (self.class_name, '_get_ads_info_by_adset', adset_id, ads_info))
                return ads_info
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, adset_id:%s, reason:%s' % \
                (self.class_name, '_get_ads_info_by_adset', adset_id, why))
            return False



    """
    获取Ad Creatives 信息
    """
    def _get_ad_creatives(self, ad_id):
        try:
            ad_creatives = self.fb_api.get_adcreative_by_adid(ad_id)
            if ad_creatives:
                self.logger.info('[%s] [%s] Info, ad_id:%s, ad_creatives:%s' % \
                    (self.class_name, '_get_ad_creatives', ad_id, ad_creatives))
                return ad_creatives
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, ad_id:%s, reason:%s' % \
                (self.class_name, '_get_ad_creatives', ad_id, why))
            return False


    """
    根据Account id 获取campaign insights数据
    """
    def _get_campaign_insights_by_account(self, account_id, breakdown_attribute, start_dt, stop_dt):
        try:
            campaign_insights = self.fb_api.get_campaign_insights_by_account(account_id, breakdown_attribute, start_dt, stop_dt)
            if campaign_insights:
                self.logger.info('[%s] [%s] Info, account_id:%s, start_dt:%s, stop_dt:%s, campaign_insights:%s' % \
                    (self.class_name, '_get_campaign_insights_by_account', account_id, start_dt, stop_dt, campaign_insights))
                return campaign_insights
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, account_id:%s, start_dt:%s, stop_dt:%s, reason:%s' % \
                (self.class_name, '_get_campaign_insights_by_account', account_id, start_dt, stop_dt, why))
            return False



    """
    根据Campaign id 获取adset insights数据
    """
    def _get_adset_insights_by_campaign(self, campaign_id, breakdown_attribute, start_dt, stop_dt):
        try:
            campaign_insights = self.fb_api.get_adset_insight_by_campaign(campaign_id, breakdown_attribute, start_dt, stop_dt)
            if campaign_insights:
                self.logger.info('[%s] [%s] Info, campaign_id:%s, start_dt:%s, stop_dt:%s, campaign_insights:%s' % \
                    (self.class_name, '_get_adset_insights_by_campaign', campaign_id, start_dt, stop_dt, campaign_insights))
                return campaign_insights
            return {}
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, campaign_id:%s, start_dt:%s, stop_dt:%s, reason:%s' % \
                (self.class_name, '_get_adset_insights_by_campaign', campaign_id, start_dt, stop_dt, why))
            return False


    """
    根据object_story_id获取电商link，很奇怪sdk中没有相应接口
    """
    def _get_ecommerce_link(self, object_story_id):
        try:
            url = 'https://graph.facebook.com/%s/%s' % (self.api_version, object_story_id)
            params = {
                'fields':'link',
                'access_token':self.access_token
            }
            response = requests.get(url, params=params).text
            if response:
                object_story = json.loads(response)
                if "link" in object_story.keys(): return object_story["link"]
            return ''
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, object_story_id:%s, reason:%s' % \
                (self.class_name, '_get_ecommerce_link', object_story_id, why))
            return False



    """
    按格式构建创意数据
    [image_url、image_hash、video_id、promoted_url]
    """
    def _build_creative_data(self, ad_creatives):
        try:
            creative_data_list = []
            if ad_creatives:
                #整合创意信息
                for ad_creative in ad_creatives:
                    link = '' if "object_story_id" not in ad_creative.keys() else self._get_ecommerce_link(ad_creative["object_story_id"])
                    if "object_type" in ad_creative.keys():
                        object_type			= ad_creative["object_type"]
                        object_story_spec	= {} if "object_story_spec" not in ad_creative.keys() else ad_creative["object_story_spec"]
                        #判断创意的类型
                        #如果为视频类的创意
                        if object_type == "VIDEO":
                            video_data			= {} if "video_data" not in object_story_spec.keys() else object_story_spec["video_data"]
                            call_to_action		= {} if "call_to_action" not in video_data.keys() else video_data["call_to_action"]
                            image_url			= '' if "image_url" not in video_data.keys() else video_data["image_url"]
                            image_hash			= ''
                            video_id			= '' if "video_id" not in video_data.keys() else video_data["video_id"]
                            value				= {} if "value" not in call_to_action.keys() else call_to_action["value"]
                            promoted_url		= '' if "link" not in value.keys() else value["link"]
                        #如果为非视频类的创意，如SHARE
                        elif object_type == "SHARE":
                            link_data			= {} if "link_data" not in object_story_spec.keys() else object_story_spec["link_data"]
                            call_to_action		= {} if "call_to_action"not in link_data.keys() else link_data["call_to_action"]
                            image_url			= '' if "image_url" not in ad_creative.keys() else ad_creative["image_url"]
                            image_hash			= '' if "image_hash" not in ad_creative.keys() else ad_creative["image_hash"]
                            video_id			= ''
                            promoted_url		= '' if "link" not in link_data.keys() else link_data["link"]
                        #其他类型先不管了，详情：https://developers.facebook.com/docs/marketing-api/reference/ad-creative
                        else:
                            continue
                    else:
                        raise Exception('Unknow ad creative object type:%s, object_story_spec:%s' % (object_type, object_story_spec))
                    creative_data_list.extend([image_url, image_hash, video_id, promoted_url, link])
                    self.logger.info('[%s] [%s] Info, ad_creatives:%s, creative_data_list:%s' % \
                        (self.class_name, '_build_creative_data', ad_creatives, creative_data_list))
                    break
            return creative_data_list
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, ad_creatives:%s, reason:%s' % \
                (self.class_name, '_build_creative_data', ad_creatives, why))
            return False



# vim: set noexpandtab ts=4 sts=4 sw=4 :
